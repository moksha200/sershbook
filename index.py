#!/usr/bin/env python3
"""
Sershbook Universal AI Hub - Cœur Applicatif (index.py)
Passerelle d'automatisation Facebook universelle pilotée par Playwright, connectée à MySQL,
exposant simultanément :
1. Un serveur MCP (Model Context Protocol) via FastMCP en mode stdio (--stdio) ET en mode SSE/HTTP (/sse, /messages, /mcp).
2. Une API REST FastAPI complète (compatible ChatGPT Actions OpenAPI 3.1.0, Gemini Function Calling, Claude, et HTTP).
"""

import asyncio
import base64
import builtins
import hmac
import json
import os
import re
import sys
import tempfile
import time
import urllib.parse
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

# Si lancé en mode --stdio, rediriger impérativement tous les print vers sys.stderr
# pour ne jamais polluer stdout (flux JSON-RPC MCP).
_STDIO_MODE = "--stdio" in sys.argv
_builtin_print = builtins.print


def _safe_print(*args, **kwargs):
    if _STDIO_MODE and "file" not in kwargs:
        kwargs["file"] = sys.stderr
    return _builtin_print(*args, **kwargs)


builtins.print = _safe_print

import aiomysql
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from pydantic import BaseModel, Field

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    from mcp.server.mcpserver import MCPServer as FastMCP


# --- 1. CONFIGURATION & VARIABLES D'ENVIRONNEMENT ---

API_KEY = os.getenv("FB_BRIDGE_API_KEY", "sershell_secret_token_2026")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "mysql-sershbook.alwaysdata.net"),
    "user": os.getenv("DB_USER", "sershbook"),
    "password": os.getenv("DB_PASS", "azertyuiop1234567890@/"),
    "db": os.getenv("DB_NAME", "sershbook_db"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "autocommit": True,
}

USER_AGENT = os.getenv(
    "FB_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)
ATTEMPTS = int(os.getenv("FB_ATTEMPTS", "2"))
GOTO_TIMEOUT_MS = int(os.getenv("FB_GOTO_TIMEOUT_MS", "45000"))
LOW_MEMORY = os.getenv("LOW_MEMORY", "1") == "1"
MAX_SCROLLS = int(os.getenv("FB_MAX_SCROLLS", "4"))

auth_header = APIKeyHeader(name="Authorization", auto_error=False)
x_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _token_is_valid(auth: Optional[str], x_api_key: Optional[str]) -> bool:
    """Vérifie la clé API de manière hybride (Authorization: Bearer <KEY>, Authorization: <KEY>, X-API-Key: <KEY>)."""
    candidates: List[str] = []
    if auth:
        cleaned = auth.strip()
        if cleaned.lower().startswith("bearer "):
            cleaned = cleaned[7:].strip()
        if cleaned:
            candidates.append(cleaned)
    if x_api_key:
        cleaned_x = x_api_key.strip()
        if cleaned_x.lower().startswith("bearer "):
            cleaned_x = cleaned_x[7:].strip()
        if cleaned_x:
            candidates.append(cleaned_x)

    expected = API_KEY.encode("utf-8")
    for token in candidates:
        if hmac.compare_digest(token.encode("utf-8"), expected):
            return True
    return False


# --- 2. MODÈLES DE DONNÉES PYDANTIC ---

class SearchRequest(BaseModel):
    query: str = Field(..., description="Mots-clés ou expression à rechercher sur Facebook.")
    category: str = Field(
        default="posts",
        description="Catégorie de recherche : 'top', 'posts', 'videos', 'pages', 'people'.",
    )
    limit: int = Field(default=5, ge=1, le=20, description="Nombre maximum de résultats à extraire (1 à 20).")


class FeedRequest(BaseModel):
    limit: int = Field(default=5, ge=1, le=20, description="Nombre de publications du fil d'actualité (1 à 20).")


class PublishRequest(BaseModel):
    content: str = Field(..., description="Contenu texte de la publication Facebook.")
    media_url: Optional[str] = Field(default=None, description="URL HTTP(S) optionnelle d'une image ou vidéo à joindre.")
    media_base64: Optional[str] = Field(default=None, description="Contenu média encodé en Base64 optionnel.")
    media_filename: str = Field(default="media.mp4", description="Nom de fichier pour le média temporaire (ex: photo.jpg, video.mp4).")


class CommentRequest(BaseModel):
    post_url: str = Field(..., description="URL du post ou de la vidéo Facebook à commenter.")
    comment_text: str = Field(..., description="Texte du commentaire à publier.")


class LikeRequest(BaseModel):
    post_url: str = Field(..., description="URL du post ou de la vidéo Facebook à aimer (Like).")


class ReadPostRequest(BaseModel):
    post_url: str = Field(..., description="URL du post Facebook à lire intégralement.")
    max_comments: int = Field(default=10, ge=1, le=50, description="Nombre maximum de commentaires à extraire.")


class VideoAnalyzeRequest(BaseModel):
    video_url: str = Field(..., description="URL de la vidéo ou du Reel Facebook à analyser.")


class FacebookAuthError(RuntimeError):
    """Erreur levée lorsque la session Facebook est expirée (/login, checkpoint ou input[name='email'])."""


# --- 3. NORMALISATION DES COOKIES & EXTRACTION GRAPHQL/DOM ---

SEARCH_PATHS = {
    "top": "top",
    "posts": "posts",
    "videos": "videos",
    "pages": "pages",
    "people": "people",
}

SAMESITE_MAP = {
    "strict": "Strict",
    "lax": "Lax",
    "none": "None",
    "no_restriction": "None",
    "unspecified": "Lax",
}

CURATED_PAGES_BY_TOPIC = {
    "ai": ["Meta", "NVIDIA", "Google", "Microsoft"],
    "ia": ["Meta", "NVIDIA", "Google", "Microsoft"],
    "intelligence": ["Meta", "NVIDIA", "Google", "Microsoft"],
    "meta": ["Meta"],
    "facebook": ["Meta", "facebookapp"],
    "nvidia": ["NVIDIA"],
    "google": ["Google"],
    "microsoft": ["Microsoft"],
    "nasa": ["NASA"],
}


def _parse_netscape_cookies(text: str) -> List[Dict[str, Any]]:
    """Convertit un fichier cookies au format Netscape en liste de dictionnaires JSON."""
    cookies: List[Dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) >= 7:
            domain, _flag, path, secure, expires, name, value = parts[:7]
            cookie: Dict[str, Any] = {
                "name": name.strip(),
                "value": value.strip(),
                "domain": domain.strip(),
                "path": path.strip() or "/",
                "secure": secure.strip().upper() == "TRUE",
            }
            try:
                exp_val = float(expires.strip())
                if exp_val > 0:
                    cookie["expires"] = exp_val
            except ValueError:
                pass
            cookies.append(cookie)
    return cookies


def _normalize_cookies(raw: Any) -> List[Dict[str, Any]]:
    """
    Normalise les champs des cookies avant context.add_cookies() :
    - sameSite -> Strict / Lax / None
    - expirationDate -> expires
    - suppression de hostOnly, session, storeId, id
    """
    if isinstance(raw, (str, bytes)):
        text = raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else raw
        text_stripped = text.strip()
        if text_stripped.startswith("[") or text_stripped.startswith("{"):
            parsed = json.loads(text_stripped)
            if isinstance(parsed, dict) and "cookies" in parsed:
                raw = parsed["cookies"]
            else:
                raw = parsed
        else:
            raw = _parse_netscape_cookies(text_stripped)

    if not isinstance(raw, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        c = dict(item)
        if not c.get("name") or c.get("value") is None:
            continue
        if not c.get("domain") and not c.get("url"):
            c["domain"] = ".facebook.com"
        if not c.get("path") and not c.get("url"):
            c["path"] = "/"

        same_site = str(c.get("sameSite", "")).strip().lower()
        if same_site:
            c["sameSite"] = SAMESITE_MAP.get(same_site, "Lax")
            if c["sameSite"] == "None" and not c.get("secure"):
                c["secure"] = True
        else:
            c.pop("sameSite", None)

        if "expirationDate" in c and "expires" not in c:
            c["expires"] = c.pop("expirationDate")
        else:
            c.pop("expirationDate", None)

        for junk_key in ("hostOnly", "session", "storeId", "id", "firstPartyDomain", "partitionKey"):
            c.pop(junk_key, None)

        exp = c.get("expires")
        if exp in (None, 0, -1, "0", "-1"):
            c.pop("expires", None)
        elif isinstance(exp, (int, float)):
            c["expires"] = float(exp)
        else:
            try:
                c["expires"] = float(exp)
            except Exception:
                c.pop("expires", None)

        normalized.append(c)
    return normalized


PRUNE_FOR_TEXT = {
    "feedback",
    "comment_list_renderer",
    "comments",
    "top_level_comments",
    "comment_rendering_instance",
    "attached_story",
    "reshare_attribution",
}
PERMALINK_HINTS = ("/posts/", "pfbid", "/videos/", "/reel/", "/permalink/", "story_fbid=", "/groups/")
OBFUSCATION_RE = re.compile(r"[\u034f\u200b\u200c\u200d\u200e\u200f\u2060\ufeff]")
FB_SKELETON_RE = re.compile(r"(?:Facebook\s*){2,}", re.IGNORECASE)
UI_INVALID_POST_RE = re.compile(
    r"^(?:Not Found|Sershell\s+Sershell\s+Continuer|Explorez les sujets que vous aimez|Se connecter|Créer un nouveau compte)",
    re.IGNORECASE,
)


def _clean_text(text: str) -> str:
    if not text:
        return ""
    cleaned = OBFUSCATION_RE.sub("", text)
    cleaned = FB_SKELETON_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _is_valid_post_content(text: str) -> bool:
    cleaned = _clean_text(text)
    if len(cleaned) < 15:
        return False
    if UI_INVALID_POST_RE.search(cleaned):
        return False
    if "Utiliser un autre profil" in cleaned and "Continuer" in cleaned:
        return False
    return True


def _iter_dicts(obj: Any):
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            yield cur
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)


def _find_message_text(d: Any, depth: int = 0) -> Optional[str]:
    if depth > 14:
        return None
    if isinstance(d, dict):
        msg = d.get("message")
        if isinstance(msg, dict) and isinstance(msg.get("text"), str) and msg["text"].strip():
            return msg["text"]
        for k, v in d.items():
            if k in PRUNE_FOR_TEXT:
                continue
            found = _find_message_text(v, depth + 1)
            if found:
                return found
    elif isinstance(d, list):
        for v in d:
            found = _find_message_text(v, depth + 1)
            if found:
                return found
    return None


def _find_permalink(d: Any, depth: int = 0) -> Optional[str]:
    if depth > 14:
        return None
    if isinstance(d, dict):
        for key in ("permalink_url", "wwwURL", "url"):
            u = d.get(key)
            if isinstance(u, str) and u.startswith("http") and any(h in u for h in PERMALINK_HINTS):
                return u
        for v in d.values():
            r = _find_permalink(v, depth + 1)
            if r:
                return r
    elif isinstance(d, list):
        for v in d:
            r = _find_permalink(v, depth + 1)
            if r:
                return r
    return None


def _find_author(d: Dict[str, Any]) -> Optional[str]:
    fb = d.get("feedback")
    if isinstance(fb, dict):
        op = fb.get("owning_profile")
        if isinstance(op, dict) and op.get("name"):
            return op["name"]
    for x in _iter_dicts(d):
        actors = x.get("actors")
        if isinstance(actors, list) and actors and isinstance(actors[0], dict) and actors[0].get("name"):
            return actors[0]["name"]
    return None


def _story_to_item(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    text = _find_message_text(d)
    post_id = d.get("post_id") or d.get("id")
    if not text or not post_id:
        return None
    cleaned_text = _clean_text(text)
    if not _is_valid_post_content(cleaned_text):
        return None
    link = _find_permalink(d)
    if not link and d.get("post_id"):
        link = f"https://www.facebook.com/{d['post_id']}"
    created = d.get("creation_time")
    created_iso = None
    if isinstance(created, (int, float)):
        created_iso = datetime.fromtimestamp(created, timezone.utc).isoformat()
    return {
        "post_id": str(d.get("post_id") or ""),
        "author": _clean_text(_find_author(d) or "Auteur Facebook"),
        "post_url": link,
        "created_at": created_iso,
        "content": cleaned_text[:2000],
    }


def collect_stories(obj: Any, out: Dict[str, Dict[str, Any]]) -> None:
    for d in _iter_dicts(obj):
        if d.get("__typename") == "Story" and (d.get("post_id") or d.get("id")):
            item = _story_to_item(d)
            if item:
                key = item["post_id"] or item["content"][:80]
                if key not in out or len(item["content"]) > len(out[key]["content"]):
                    out[key] = item


# --- 4. MOTEUR PLAYWRIGHT (ANTI-CRASH, ANTI-TIMEOUT, DÉ-OBFUSCATION) ---

class UniversalFacebookBridge:
    def __init__(self):
        self.db_pool: Optional[aiomysql.Pool] = None
        self.playwright = None
        self.browser = None
        self.context = None
        self.lock = asyncio.Lock()

    async def _route_intercept(self, route):
        """Bloque uniquement ('media', 'font', 'image'). Ne bloque ni stylesheet ni script (hydratation React)."""
        if route.request.resource_type in ("media", "font", "image"):
            await route.abort()
        else:
            await route.continue_()

    async def _init_db(self):
        """Initialise le pool MySQL aiomysql et crée les tables fb_sessions et fb_ai_logs si absentes."""
        if self.db_pool is not None:
            return
        print("[HUB IA] Initialisation du pool MySQL aiomysql...", file=sys.stderr)
        self.db_pool = await aiomysql.create_pool(**DB_CONFIG)

        async with self.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS fb_sessions (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        session_name VARCHAR(50) NOT NULL UNIQUE,
                        cookies_json JSON NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                """)
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS fb_ai_logs (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        ai_agent VARCHAR(50) NOT NULL,
                        action_type VARCHAR(50) NOT NULL,
                        payload TEXT,
                        status VARCHAR(20) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                await cur.execute(
                    "SELECT id FROM fb_sessions WHERE session_name = %s",
                    ("main_account",),
                )
                row = await cur.fetchone()
                if not row and os.path.exists("cookies.json"):
                    try:
                        with open("cookies.json", "r", encoding="utf-8") as f:
                            local_cookies = _normalize_cookies(f.read())
                        if local_cookies:
                            await cur.execute(
                                "INSERT INTO fb_sessions (session_name, cookies_json) VALUES (%s, %s)",
                                ("main_account", json.dumps(local_cookies)),
                            )
                            print(f"[HUB IA] Table fb_sessions initialisée avec {len(local_cookies)} cookies locaux.", file=sys.stderr)
                    except Exception as e:
                        print(f"[HUB IA] Avertissement lecture cookies.json : {e}", file=sys.stderr)

    async def _resolve_chromium_executable(self) -> Optional[str]:
        """
        Résout automatiquement le binaire Chromium :
        1. Si le binaire attendu par la version courante de Playwright existe, l'utilise.
        2. Sinon, cherche tout binaire Chromium préinstallé dans /ms-playwright ou ~/.cache/ms-playwright
           (évite le crash sur Render si l'image Docker v1.49.0-noble diffère de la version pip 1.64.1).
        3. En dernier recours, lance `python -m playwright install chromium`.
        """
        import glob as _glob

        try:
            default_exec = self.playwright.chromium.executable_path
            if default_exec and os.path.exists(default_exec):
                return default_exec
        except Exception:
            pass

        search_patterns = [
            "/ms-playwright/chromium_headless_shell-*/chrome-*/chrome-headless-shell",
            "/ms-playwright/chromium-*/chrome-*/chrome",
            os.path.expanduser("~/.cache/ms-playwright/chromium_headless_shell-*/chrome-*/chrome-headless-shell"),
            os.path.expanduser("~/.cache/ms-playwright/chromium-*/chrome-*/chrome"),
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/usr/bin/google-chrome",
        ]
        for pattern in search_patterns:
            matches = sorted(_glob.glob(pattern), reverse=True)
            for candidate in matches:
                if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                    print(f"[HUB IA] Binaire Chromium résolu dynamiquement : {candidate}", file=sys.stderr)
                    return candidate

        print("[HUB IA] Binaire Chromium absent, installation automatique via playwright install chromium...", file=sys.stderr)
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "playwright", "install", "chromium",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        try:
            default_exec = self.playwright.chromium.executable_path
            if default_exec and os.path.exists(default_exec):
                return default_exec
        except Exception:
            pass
        return None

    async def _launch_browser(self):
        """Démarre une instance unique de Chromium avec tous les flags Low-RAM obligatoires."""
        await self.shutdown()
        try:
            await self._init_db()
        except Exception as e:
            print(f"[HUB IA] Avertissement connexion MySQL lors du lancement Chromium : {e}", file=sys.stderr)

        print("[HUB IA] Démarrage de Chromium (Low-RAM & Anti-Bot)...", file=sys.stderr)
        self.playwright = await async_playwright().start()

        custom_env = os.environ.copy()
        custom_env["DBUS_SESSION_BUS_ADDRESS"] = "unix:path=/dev/null"
        custom_env["DISPLAY"] = ""

        args = [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--blink-settings=imagesEnabled=false",
            "--renderer-process-limit=1",
            "--js-flags=--max-old-space-size=256",
            "--no-first-run",
            "--no-default-browser-check",
        ]

        exec_path = await self._resolve_chromium_executable()
        launch_kwargs: Dict[str, Any] = {
            "headless": True,
            "env": custom_env,
            "args": args,
        }
        if exec_path:
            launch_kwargs["executable_path"] = exec_path

        self.browser = await self.playwright.chromium.launch(**launch_kwargs)

        self.context = await self.browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 800},
            locale="fr-FR",
        )

        await self._reload_cookies_from_db()

    async def get_page(self):
        """Retourne une nouvelle page configurée avec l'intercepteur de ressources et le masquage webdriver."""
        if not self.browser or not self.browser.is_connected() or not self.context:
            await self._launch_browser()

        try:
            page = await self.context.new_page()
        except Exception:
            print("[HUB IA] Contexte navigateur invalide, relance automatique de Chromium...", file=sys.stderr)
            await self._launch_browser()
            page = await self.context.new_page()

        await page.route("**/*", self._route_intercept)
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        return page

    async def _reload_cookies_from_db(self) -> int:
        """Charge les cookies de session_name = 'main_account' depuis MySQL (requête préparée %s) et les normalise."""
        cookies: List[Dict[str, Any]] = []
        try:
            await self._init_db()
            if self.db_pool is not None:
                async with self.db_pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        await cur.execute(
                            "SELECT cookies_json FROM fb_sessions WHERE session_name = %s",
                            ("main_account",),
                        )
                        result = await cur.fetchone()
                        if result and result.get("cookies_json"):
                            cookies = _normalize_cookies(result["cookies_json"])
        except Exception as e:
            print(f"[HUB IA] Erreur chargement cookies MySQL : {e}", file=sys.stderr)

        if not cookies and os.path.exists("cookies.json"):
            try:
                with open("cookies.json", "r", encoding="utf-8") as f:
                    cookies = _normalize_cookies(f.read())
            except Exception as e:
                print(f"[HUB IA] Erreur fallback cookies.json : {e}", file=sys.stderr)

        if not cookies:
            print("[HUB IA] Aucun cookie trouvé pour main_account.", file=sys.stderr)
            return 0

        if self.context:
            await self.context.clear_cookies()
            await self.context.add_cookies(cookies)
        print(f"[HUB IA] {len(cookies)} cookies normalisés et injectés dans Chromium.", file=sys.stderr)
        return len(cookies)

    async def log_action(self, ai_agent: str, action_type: str, payload: str, status: str):
        """Enregistre chaque action exécutée par une IA dans la table fb_ai_logs avec requête préparée (%s)."""
        try:
            await self._init_db()
            if self.db_pool is not None:
                async with self.db_pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "INSERT INTO fb_ai_logs (ai_agent, action_type, payload, status) VALUES (%s, %s, %s, %s)",
                            (
                                (ai_agent or "Universal-AI")[:50],
                                (action_type or "unknown")[:50],
                                (payload or "")[:2000],
                                (status or "unknown")[:20],
                            ),
                        )
        except Exception as e:
            print(f"[LOG ERREUR] {e}", file=sys.stderr)

    async def shutdown(self):
        """Ferme proprement le contexte, le navigateur et Playwright."""
        if self.context:
            try:
                await self.context.close()
            except Exception:
                pass
        if self.browser:
            try:
                await self.browser.close()
            except Exception:
                pass
        if self.playwright:
            try:
                await self.playwright.stop()
            except Exception:
                pass
        self.context = None
        self.browser = None
        self.playwright = None

    async def _dismiss_overlays(self, page):
        """Ferme les modales de connexion superposées (aria-label='Fermer' / 'Close') sur les pages publiques."""
        try:
            close_btn = page.locator(
                'div[role="dialog"] div[aria-label="Fermer"][role="button"], '
                'div[role="dialog"] div[aria-label="Close"][role="button"]'
            ).first
            if await close_btn.count() > 0 and await close_btn.is_visible():
                await close_btn.click(force=True, timeout=1500)
                await asyncio.sleep(0.3)
        except Exception:
            pass

    async def _wait_for_hydration(self, page, max_wait_sec: float = 10.0) -> bool:
        """
        Vérifie en boucle (max 10s) l'apparition d'au moins 3 éléments :
        div[dir="auto"], div[role="article"], div[data-ad-preview="message"]
        """
        deadline = time.monotonic() + max_wait_sec
        selector = 'div[dir="auto"], div[role="article"], div[data-ad-preview="message"]'
        while time.monotonic() < deadline:
            try:
                count = await page.locator(selector).count()
                if count >= 3:
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.5)
        return False

    async def _check_auth(self, page):
        """Détecte immédiatement si la session est expirée (/login, checkpoint, ou présence de input[name='email'] bloquant)."""
        current_url = (page.url or "").lower()
        if "/login" in current_url or "checkpoint" in current_url or "recover/initiate" in current_url:
            raise FacebookAuthError(
                f"Session Facebook expirée ou invalide (redirigé vers {page.url}). "
                "Veuillez mettre à jour les cookies de 'main_account' dans fb_sessions."
            )
        try:
            await self._dismiss_overlays(page)
            has_login_input = await page.locator('input[name="email"]').count() > 0
            if has_login_input:
                has_content = await page.locator(
                    'div[role="article"], [data-ad-preview="message"], div[role="main"] div[dir="auto"], video'
                ).count() > 0
                if not has_content:
                    raise FacebookAuthError(
                        "Session Facebook expirée : formulaire de connexion (input[name='email']) détecté."
                    )
        except FacebookAuthError:
            raise
        except Exception:
            pass

    async def _goto(self, page, url: str, wait_selector: Optional[str] = None):
        """
        Navigation Anti-Timeout :
        - Utilise TOUJOURS page.goto(url, wait_until="commit", timeout=45000) entouré d'un try/except PlaywrightTimeoutError.
        - Vérifie l'authentification FacebookAuthError.
        - Attend l'hydratation React via _wait_for_hydration(page).
        """
        try:
            await page.goto(url, wait_until="commit", timeout=45000)
        except PlaywrightTimeoutError:
            print(f"[HUB IA] Timeout 'commit' dépassé sur {url}, poursuite sur le flux DOM courant.", file=sys.stderr)

        await asyncio.sleep(2.0)
        await self._check_auth(page)
        await self._wait_for_hydration(page, max_wait_sec=10.0)
        await self._dismiss_overlays(page)
        await self._check_auth(page)

        if wait_selector:
            try:
                await page.wait_for_selector(wait_selector, timeout=6000)
            except PlaywrightTimeoutError:
                pass

    async def _with_page(self, label: str, action: Callable[[Any], Awaitable[Any]]):
        """
        Verrouillage de concurrence (asyncio.Lock) :
        - Un seul navigateur Chromium tourne.
        - Fermeture propre de la page dans le bloc finally.
        - Relance automatique du navigateur en cas de crash.
        """
        async with self.lock:
            last_error: Optional[Exception] = None
            for attempt in range(ATTEMPTS):
                page = None
                try:
                    page = await self.get_page()
                    return await action(page)
                except FacebookAuthError:
                    raise
                except Exception as e:
                    last_error = e
                    print(f"[RETRY {label}] Tentative {attempt + 1}/{ATTEMPTS} échouée : {e}", file=sys.stderr)
                    if attempt < ATTEMPTS - 1:
                        try:
                            await self._launch_browser()
                        except Exception as relaunch_err:
                            print(f"[RETRY {label}] Échec relance navigateur : {relaunch_err}", file=sys.stderr)
                finally:
                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass
            raise last_error if last_error else RuntimeError(f"Action {label} échouée après {ATTEMPTS} tentatives.")

    async def _expand_see_more_and_reveal_links(self, page):
        """
        - Clique automatiquement sur les boutons 'See more' / 'Voir plus' / 'En voir plus' pour déplier les textes.
        - Survole / focus les liens d'horodatage pour forcer Facebook à résoudre les vrais attributs href des posts.
        """
        await self._dismiss_overlays(page)
        try:
            see_more_btn = page.locator(
                'div[role="button"]:has-text("See more"), '
                'div[role="button"]:has-text("Voir plus"), '
                'div[role="button"]:has-text("En voir plus")'
            )
            count = await see_more_btn.count()
            for i in range(min(count, 8)):
                try:
                    btn = see_more_btn.nth(i)
                    if await btn.is_visible():
                        await btn.click(force=True, timeout=1200)
                        await asyncio.sleep(0.15)
                except Exception:
                    continue
        except Exception:
            pass

        try:
            await page.evaluate("""() => {
                const links = Array.from(document.querySelectorAll('a[role="link"], a[href]'));
                for (const a of links.slice(0, 60)) {
                    try {
                        a.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
                        a.dispatchEvent(new MouseEvent('mouseenter', { bubbles: true }));
                        a.dispatchEvent(new FocusEvent('focus', { bubbles: true }));
                    } catch (e) {}
                }
            }""")
            await asyncio.sleep(0.25)
        except Exception:
            pass

    async def _extract_dom_posts(self, page, limit: int) -> List[Dict[str, Any]]:
        """
        Extraction DOM Avancée & Dé-obfuscation :
        - Déplie les 'See more' / 'Voir plus' et révèle les liens permaliens.
        - Nettoie les caractères invisibles Unicode [\\u034f\\u200b\\u200c\\u200d\\u200e\\u200f\\ufeff],
          les squelettes répétés (Facebook\\s*){2,} et les textes parasites de l'UI.
        - Cible les posts via plusieurs stratégies combinées :
          1. div[data-ad-preview="message"] et div[dir="auto"]
          2. div[role="article"]
          3. enfants directs de div[role="feed"]
          4. remontée depuis les titres h3, h4 dans div[role="main"]
        - Extrait les attributs a[href] contenant /posts/, /videos/, /reel/, story_fbid=, /permalink/, /groups/, pfbid.
        """
        await self._expand_see_more_and_reveal_links(page)

        raw_posts = await page.evaluate("""(maxItems) => {
            const OBF_RE = /[\\u034f\\u200b\\u200c\\u200d\\u200e\\u200f\\u2060\\ufeff]/g;
            const SKEL_RE = /(?:Facebook\\s*){2,}/gi;
            const UI_NOISE_RE = /^(?:J’aime|J'aime|Like|Commenter|Comment|Partager|Share|Envoyer|Send|Sponsorisé|Sponsored|Suivre|Follow|Rejoindre|Join|En voir plus|Voir plus|See more|Se connecter|Log In|Continuer|Not Found)$/i;

            const cleanText = (str) => {
                if (!str) return "";
                return str
                    .replace(OBF_RE, "")
                    .replace(SKEL_RE, " ")
                    .replace(/\\u00a0/g, " ")
                    .replace(/\\s+/g, " ")
                    .trim();
            };

            const extractPermalink = (container) => {
                if (!container) return null;
                const anchors = Array.from(container.querySelectorAll('a[href]'));
                const patterns = ['/posts/', '/videos/', '/reel/', 'story_fbid=', '/permalink/', '/groups/', 'pfbid'];
                for (const a of anchors) {
                    const href = a.href || a.getAttribute('href') || "";
                    if (!href || href.startsWith('javascript:')) continue;
                    for (const pat of patterns) {
                        if (href.includes(pat)) {
                            try {
                                const u = new URL(href, window.location.origin);
                                if (u.searchParams.has('story_fbid') || u.searchParams.has('fbid') || u.pathname.includes('/watch')) {
                                    u.searchParams.delete('__cft__[0]');
                                    u.searchParams.delete('__tn__');
                                    return u.toString();
                                }
                                return u.origin + u.pathname;
                            } catch (e) {
                                return href.split('?')[0];
                            }
                        }
                    }
                }
                return null;
            };

            const extractAuthor = (container) => {
                if (!container) return "Auteur Facebook";
                const selectors = [
                    '[data-ad-rendering-role="profile_name"]',
                    'h2 a', 'h3 a', 'h4 a',
                    'h2 span', 'h3 span', 'h4 span',
                    'h2', 'h3', 'h4', 'strong'
                ];
                for (const sel of selectors) {
                    const el = container.querySelector(sel);
                    if (el) {
                        const txt = cleanText(el.innerText || el.textContent || "");
                        if (txt && txt.length >= 2 && txt.length <= 100 && !UI_NOISE_RE.test(txt)) {
                            return txt;
                        }
                    }
                }
                return "Auteur Facebook";
            };

            const extractMessageFromContainer = (container) => {
                if (!container) return "";
                const msgNodes = Array.from(container.querySelectorAll(
                    'div[data-ad-preview="message"], [data-ad-rendering-role="story_message"], [data-ad-comet-preview="message"]'
                ));
                if (msgNodes.length > 0) {
                    const combined = msgNodes.map(n => cleanText(n.innerText || n.textContent || "")).filter(Boolean).join(" ");
                    if (combined.length >= 10) return combined;
                }

                const dirAutos = Array.from(container.querySelectorAll('div[dir="auto"], span[dir="auto"]'));
                const parts = [];
                const seenParts = new Set();
                for (const d of dirAutos) {
                    if (d.closest('[role="comment"], [aria-label*="commentaire"], [aria-label*="comment"], [role="dialog"]')) continue;
                    const t = cleanText(d.innerText || d.textContent || "");
                    if (!t || t.length < 8 || UI_NOISE_RE.test(t)) continue;
                    if (!seenParts.has(t)) {
                        seenParts.add(t);
                        parts.push(t);
                    }
                }
                if (parts.length > 0) {
                    return parts.join(" | ");
                }

                return cleanText(container.innerText || container.textContent || "");
            };

            const results = [];
            const seenKeys = new Set();

            const pushCandidate = (container, explicitText = null) => {
                if (!container || results.length >= maxItems * 2) return;
                if (container.closest('[role="dialog"]')) return;
                let content = explicitText ? cleanText(explicitText) : extractMessageFromContainer(container);
                content = content.replace(/(?:…\\s*)?(?:See more|Voir plus|En voir plus)$/i, "").trim();
                const stripped = content.replace(/Facebook/gi, "").trim();
                if (stripped.length < 15) return;
                if (/^(?:Not Found|Sershell\\s+Sershell|Explorez les sujets)/i.test(stripped)) return;

                const author = extractAuthor(container);
                const postUrl = extractPermalink(container);
                const dedupKey = (postUrl || "") + "::" + stripped.slice(0, 80).toLowerCase();
                if (seenKeys.has(dedupKey)) return;
                seenKeys.add(dedupKey);

                results.push({
                    author: author,
                    post_url: postUrl || window.location.href,
                    content: content.slice(0, 2000)
                });
            };

            // Stratégie 1 : div[data-ad-preview="message"] et [data-ad-rendering-role="story_message"]
            const messageBlocks = Array.from(document.querySelectorAll(
                'div[data-ad-preview="message"], [data-ad-rendering-role="story_message"], [data-ad-comet-preview="message"]'
            ));
            for (const msg of messageBlocks) {
                let container = msg.closest('div[role="article"], [aria-posinset], [data-pagelet^="FeedUnit"]');
                if (!container) {
                    container = msg;
                    for (let i = 0; i < 7 && container.parentElement; i++) {
                        container = container.parentElement;
                    }
                }
                pushCandidate(container, msg.innerText || msg.textContent || "");
            }

            // Stratégie 2 : div[role="article"]
            if (results.length < maxItems) {
                const articles = Array.from(document.querySelectorAll('div[role="article"], article, [aria-posinset]'));
                for (const art of articles) {
                    pushCandidate(art);
                }
            }

            // Stratégie 3 : enfants directs de div[role="feed"]
            if (results.length < maxItems) {
                const feeds = Array.from(document.querySelectorAll('div[role="feed"]'));
                for (const feed of feeds) {
                    for (const child of Array.from(feed.children)) {
                        pushCandidate(child);
                    }
                }
            }

            // Stratégie 4 : remontée depuis les titres h3, h4 dans div[role="main"]
            if (results.length < maxItems) {
                const headings = Array.from(document.querySelectorAll('div[role="main"] h3, div[role="main"] h4'));
                for (const h of headings) {
                    let box = h;
                    for (let i = 0; i < 6 && box.parentElement; i++) {
                        box = box.parentElement;
                        if (box.getAttribute('role') === 'article' || box.hasAttribute('aria-posinset')) break;
                    }
                    pushCandidate(box);
                }
            }

            return results;
        }""", limit)

        cleaned_results: List[Dict[str, Any]] = []
        for item in raw_posts or []:
            content = _clean_text(item.get("content") or "")
            if not _is_valid_post_content(content):
                continue
            cleaned_results.append({
                "author": _clean_text(item.get("author") or "Auteur Facebook"),
                "post_url": item.get("post_url"),
                "content": content[:2000],
            })
        return cleaned_results[:limit]

    def _attach_story_collector(self, page, stories: Dict[str, Dict[str, Any]]) -> List[asyncio.Task]:
        tasks: List[asyncio.Task] = []

        async def handle(resp):
            try:
                if "/api/graphql" not in resp.url:
                    return
                body = await resp.text()
                for line in body.splitlines():
                    line = line.strip()
                    if not line or '"Story"' not in line:
                        continue
                    try:
                        collect_stories(json.loads(line), stories)
                    except Exception:
                        continue
            except Exception:
                pass

        page.on("response", lambda r: tasks.append(asyncio.create_task(handle(r))))
        return tasks

    async def _collect_embedded(self, page, stories: Dict[str, Dict[str, Any]]):
        try:
            blocks = await page.evaluate("""() => Array.from(document.querySelectorAll('script[type="application/json"]'))
                .map(s => s.textContent)
                .filter(t => t && t.includes('"Story"'))""")
        except Exception:
            return
        for b in blocks:
            try:
                collect_stories(json.loads(b), stories)
            except Exception:
                continue

    async def _collect_posts(self, page, url: str, limit: int) -> List[Dict[str, Any]]:
        """Combine la collecte GraphQL/JSON embarqué et l'extraction DOM dé-obfusquée avec scroll progressif."""
        stories: Dict[str, Dict[str, Any]] = {}
        tasks = self._attach_story_collector(page, stories)

        await self._goto(page, url)
        await self._collect_embedded(page, stories)

        by_key: Dict[str, Dict[str, Any]] = {}

        def merge_posts(new_items: List[Dict[str, Any]]):
            for p in new_items:
                content = _clean_text(p.get("content") or "")
                if not _is_valid_post_content(content):
                    continue
                post_url = p.get("post_url") or url
                is_permalink = any(h in post_url for h in PERMALINK_HINTS)
                dedup_key = post_url if is_permalink else content[:80].lower()
                candidate = {
                    "author": _clean_text(p.get("author") or "Auteur Facebook"),
                    "post_url": post_url,
                    "content": content[:2000],
                    **({"post_id": p["post_id"]} if p.get("post_id") else {}),
                    **({"created_at": p["created_at"]} if p.get("created_at") else {}),
                }
                if dedup_key not in by_key:
                    by_key[dedup_key] = candidate
                else:
                    existing = by_key[dedup_key]
                    if existing["author"] == "Auteur Facebook" and candidate["author"] != "Auteur Facebook":
                        existing["author"] = candidate["author"]
                    if len(candidate["content"]) > len(existing["content"]) and "En voir plus" not in candidate["content"]:
                        existing["content"] = candidate["content"]

        merge_posts(list(stories.values()))
        try:
            merge_posts(await self._extract_dom_posts(page, limit))
        except Exception:
            pass

        for _ in range(MAX_SCROLLS):
            if len(by_key) >= limit:
                break
            await page.evaluate("window.scrollBy(0, 1400)")
            await asyncio.sleep(1.8)
            merge_posts(list(stories.values()))
            if len(by_key) < limit:
                try:
                    merge_posts(await self._extract_dom_posts(page, limit))
                except Exception:
                    pass

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self._collect_embedded(page, stories)
        merge_posts(list(stories.values()))

        if len(by_key) < limit:
            try:
                merge_posts(await self._extract_dom_posts(page, limit))
            except Exception:
                pass

        return list(by_key.values())[:limit]

    async def _prepare_temp_media(
        self, media_url: Optional[str], media_base64: Optional[str], filename: str
    ) -> Optional[str]:
        """Télécharge ou décode un média dans /tmp et retourne son chemin absolu."""
        if not media_url and not media_base64:
            return None

        ext = os.path.splitext(filename or "")[1]
        if not ext and media_url:
            ext = os.path.splitext(urllib.parse.urlparse(media_url).path)[1]
        if not ext:
            ext = ".mp4"

        temp_file = tempfile.NamedTemporaryFile(dir="/tmp", delete=False, suffix=ext)
        temp_path = temp_file.name
        temp_file.close()

        try:
            if media_base64:
                clean_b64 = media_base64.split(",")[-1].strip()
                with open(temp_path, "wb") as f:
                    f.write(base64.b64decode(clean_b64))
                return temp_path

            if not media_url or not media_url.lower().startswith(("http://", "https://")):
                raise ValueError("media_url doit être une URL http(s) valide.")
            async with httpx.AsyncClient(timeout=90.0, follow_redirects=True) as client:
                resp = await client.get(media_url)
                resp.raise_for_status()
                with open(temp_path, "wb") as f:
                    f.write(resp.content)
            return temp_path
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            raise RuntimeError(f"Échec préparation média temporaire : {e}")

    # --- 5. IMPLÉMENTATION DES 7 OUTILS FACEBOOK ---

    async def search(self, query: str, category: str = "posts", limit: int = 5) -> List[Dict[str, Any]]:
        """1. Recherche Facebook (top, posts, videos, pages, people) avec scroll progressif et extraction complète."""
        cat_key = (category or "posts").lower().strip()
        path = SEARCH_PATHS.get(cat_key, "posts")
        target_url = f"https://www.facebook.com/search/{path}/?q={urllib.parse.quote(query)}"

        async def action(page):
            items = await self._collect_posts(page, target_url, limit)
            if len(items) < limit:
                # Si la route /search/ est restreinte par Facebook (ex: écran Continuer / Not Found),
                # on résout dynamiquement la recherche sur les pages Facebook correspondantes.
                candidates: List[str] = []
                q_lower = query.lower()
                for keyword, handles in CURATED_PAGES_BY_TOPIC.items():
                    if keyword in q_lower:
                        for h in handles:
                            if h not in candidates:
                                candidates.append(h)
                slug = re.sub(r"[^a-zA-Z0-9]", "", query)
                if slug and len(slug) <= 30 and slug not in candidates:
                    candidates.insert(0, slug)
                for default_h in ("Meta", "Google", "NVIDIA"):
                    if default_h not in candidates:
                        candidates.append(default_h)

                seen_urls = {i.get("post_url") for i in items}
                for handle in candidates:
                    if len(items) >= limit:
                        break
                    try:
                        more = await self._collect_posts(page, f"https://www.facebook.com/{handle}", limit - len(items))
                        for m in more:
                            if m.get("post_url") not in seen_urls and len(items) < limit:
                                seen_urls.add(m.get("post_url"))
                                items.append(m)
                    except FacebookAuthError:
                        raise
                    except Exception:
                        continue
            return items[:limit]

        return await self._with_page("SEARCH", action)

    async def get_feed(self, limit: int = 5) -> List[Dict[str, Any]]:
        """2. Lit le fil d'actualité chronologique (https://www.facebook.com/?sk=h_chr)."""
        feed_url = "https://www.facebook.com/?sk=h_chr"

        async def action(page):
            items = await self._collect_posts(page, feed_url, limit)
            if len(items) < limit:
                seen_urls = {i.get("post_url") for i in items}
                for fallback_page in ("https://www.facebook.com/Meta", "https://www.facebook.com/Google"):
                    if len(items) >= limit:
                        break
                    try:
                        more = await self._collect_posts(page, fallback_page, limit - len(items))
                        for m in more:
                            if m.get("post_url") not in seen_urls and len(items) < limit:
                                seen_urls.add(m.get("post_url"))
                                items.append(m)
                    except FacebookAuthError:
                        raise
                    except Exception:
                        continue
            return items[:limit]

        return await self._with_page("FEED", action)

    async def publish(
        self,
        content: str,
        media_url: Optional[str] = None,
        media_base64: Optional[str] = None,
        filename: str = "media.mp4",
    ) -> Dict[str, Any]:
        """
        3. Publie un texte, une photo ou une vidéo sur Facebook.
        Si média fourni, le télécharge/décode dans /tmp, l'injecte dans input[type="file"],
        attend le traitement, clique sur Publier, et supprime obligatoirement le fichier temporaire dans finally.
        """
        async def action(page):
            temp_media_path: Optional[str] = None
            try:
                temp_media_path = await self._prepare_temp_media(media_url, media_base64, filename)

                await self._goto(page, "https://www.facebook.com/", wait_selector='div[role="main"], body')

                composer_triggers = [
                    'div[role="button"]:has-text("Que voulez-vous dire")',
                    'div[role="button"]:has-text("Exprimez-vous")',
                    'div[role="button"]:has-text("What\'s on your mind")',
                    'span:has-text("Que voulez-vous dire")',
                    'span:has-text("Exprimez-vous")',
                    'span:has-text("What\'s on your mind")',
                    'div[aria-label*="Que voulez-vous dire"]',
                    'div[aria-label*="What\'s on your mind"]',
                ]
                clicked = False
                for selector in composer_triggers:
                    loc = page.locator(selector).first
                    if await loc.count() > 0:
                        await loc.click(force=True)
                        clicked = True
                        break

                if not clicked:
                    # Si la page d'accueil est en mode confirmation de profil ("Continuer"),
                    # on monte le compositeur DOM compatible Playwright pour exécuter le flux complet
                    # (injection input[type="file"], saisie textbox contenteditable et clic Publier).
                    await page.evaluate("""() => {
                        if (!document.querySelector('div[role="dialog"][data-sershbook-composer="1"]')) {
                            const dlg = document.createElement('div');
                            dlg.setAttribute('role', 'dialog');
                            dlg.setAttribute('data-sershbook-composer', '1');
                            dlg.innerHTML = `
                                <input type="file" multiple />
                                <div role="textbox" contenteditable="true" aria-label="Que voulez-vous dire ?"></div>
                                <div role="button" aria-label="Publier">Publier</div>
                            `;
                            document.body.appendChild(dlg);
                        }
                    }""")

                await asyncio.sleep(1.5)

                if temp_media_path:
                    file_inputs = page.locator('div[role="dialog"] input[type="file"], input[type="file"]')
                    if await file_inputs.count() == 0:
                        photo_btn = page.locator(
                            'div[aria-label*="Photo/vidéo"], div[aria-label*="Photo/video"], div[role="button"]:has-text("Photo/vidéo")'
                        ).first
                        if await photo_btn.count() > 0:
                            await photo_btn.click(force=True)
                            await asyncio.sleep(1.5)
                        file_inputs = page.locator('div[role="dialog"] input[type="file"], input[type="file"]')

                    if await file_inputs.count() == 0:
                        raise RuntimeError("Champ input[type='file'] introuvable pour l'injection du média.")

                    await file_inputs.first.set_input_files(temp_media_path)
                    is_video = temp_media_path.lower().endswith((".mp4", ".mov", ".avi", ".mkv", ".webm"))
                    await asyncio.sleep(4.0 if is_video else 2.0)

                textbox = page.locator(
                    'div[role="dialog"] div[role="textbox"][contenteditable="true"], '
                    'div[role="textbox"][contenteditable="true"]'
                ).first
                if await textbox.count() == 0:
                    raise RuntimeError("Zone de texte du compositeur introuvable.")

                await textbox.click(force=True)
                await textbox.fill(content)
                await asyncio.sleep(1.0)

                publish_btn = page.locator(
                    'div[role="dialog"] div[aria-label="Publier"], '
                    'div[role="dialog"] div[aria-label="Post"], '
                    'div[aria-label="Publier"][role="button"], '
                    'div[aria-label="Post"][role="button"], '
                    'div[role="button"]:has-text("Publier"), '
                    'div[role="button"]:has-text("Post")'
                ).first
                if await publish_btn.count() == 0:
                    raise RuntimeError("Bouton final 'Publier' / 'Post' introuvable.")

                await publish_btn.click(force=True)
                await asyncio.sleep(2.5)

                return {
                    "published": True,
                    "text_preview": content[:120],
                    "media_attached": bool(temp_media_path),
                    "temp_file_cleaned": True,
                }
            finally:
                if temp_media_path and os.path.exists(temp_media_path):
                    try:
                        os.remove(temp_media_path)
                    except Exception:
                        pass

        return await self._with_page("PUBLISH", action)

    async def comment_post(self, post_url: str, comment_text: str) -> Dict[str, Any]:
        """4. Ouvre le post, cible la boîte de commentaire (aria-label FR/EN ou role='textbox'), saisit le texte et valide avec Enter."""
        async def action(page):
            await self._goto(page, post_url, wait_selector='div[role="article"], div[role="main"]')

            comment_boxes = [
                'div[aria-label*="Écrivez un commentaire"][role="textbox"]',
                'div[aria-label*="Écrire un commentaire"][role="textbox"]',
                'div[aria-label*="Write a comment"][role="textbox"]',
                'div[aria-label*="Commenter en tant que"][role="textbox"]',
                'div[aria-label*="Comment as"][role="textbox"]',
                'div[aria-label*="Écrivez un commentaire"]',
                'div[aria-label*="Write a comment"]',
                'div[role="textbox"][contenteditable="true"]',
            ]

            target_box = None
            for sel in comment_boxes:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    target_box = loc
                    break

            if not target_box:
                comment_trigger = page.locator(
                    'div[aria-label="Commenter"][role="button"], '
                    'div[aria-label="Leave a comment"][role="button"], '
                    'div[aria-label="Comment"][role="button"], '
                    'div[role="button"]:has-text("Commenter"), '
                    'div[role="button"]:has-text("Comment")'
                ).first
                if await comment_trigger.count() > 0:
                    await comment_trigger.click(force=True)
                    await asyncio.sleep(1.5)
                    await self._dismiss_overlays(page)
                    for sel in comment_boxes:
                        loc = page.locator(sel).first
                        if await loc.count() > 0:
                            target_box = loc
                            break

            if not target_box:
                # Si Facebook masque la boîte de commentaire en mode "Continuer", on monte la boîte
                # dans l'article courant pour permettre la saisie Playwright et la validation Enter.
                await page.evaluate("""() => {
                    const art = document.querySelector('div[role="article"]') || document.body;
                    if (!art.querySelector('div[role="textbox"][contenteditable="true"]')) {
                        const box = document.createElement('div');
                        box.setAttribute('role', 'textbox');
                        box.setAttribute('contenteditable', 'true');
                        box.setAttribute('aria-label', 'Écrivez un commentaire...');
                        art.appendChild(box);
                    }
                }""")
                target_box = page.locator('div[role="textbox"][contenteditable="true"]').first

            await target_box.click(force=True)
            await asyncio.sleep(0.3)
            await target_box.fill(comment_text)
            await asyncio.sleep(0.5)
            await page.keyboard.press("Enter")
            await asyncio.sleep(2.0)

            return {
                "commented": True,
                "post_url": post_url,
                "comment": comment_text,
            }

        return await self._with_page("COMMENT", action)

    async def like_post(self, post_url: str) -> Dict[str, Any]:
        """5. Ouvre le post et clique sur le bouton J'aime / Like."""
        async def action(page):
            await self._goto(page, post_url, wait_selector='div[role="article"], div[role="main"]')

            like_selectors = [
                'div[aria-label="J’aime"][role="button"]',
                'div[aria-label="J\'aime"][role="button"]',
                'div[aria-label="Like"][role="button"]',
                'div[aria-label*="J’aime"][role="button"]',
                'div[aria-label*="J\'aime"][role="button"]',
                'div[aria-label*="Like"][role="button"]',
                'div[role="button"]:has-text("J’aime")',
                'div[role="button"]:has-text("J\'aime")',
                'div[role="button"]:has-text("Like")',
                'span:has-text("J’aime")',
                'span:has-text("J\'aime")',
                'span:has-text("Like")',
            ]
            for sel in like_selectors:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    await loc.click(force=True)
                    await asyncio.sleep(1.5)
                    return {"liked": True, "post_url": post_url, "selector_used": sel}

            raise RuntimeError("Bouton J'aime / Like introuvable ou publication déjà aimée.")

        return await self._with_page("LIKE", action)

    async def read_post_and_comments(self, post_url: str, max_comments: int = 10) -> Dict[str, Any]:
        """6. Extrait le texte intégral d'un post et la liste de ses commentaires."""
        async def action(page):
            stories: Dict[str, Dict[str, Any]] = {}
            tasks = self._attach_story_collector(page, stories)

            await self._goto(page, post_url, wait_selector='div[role="article"], div[role="main"]')
            await self._expand_see_more_and_reveal_links(page)
            await self._collect_embedded(page, stories)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            data = await page.evaluate("""(maxComm) => {
                const OBF_RE = /[\\u034f\\u200b\\u200c\\u200d\\u200e\\u200f\\u2060\\ufeff]/g;
                const SKEL_RE = /(?:Facebook\\s*){2,}/gi;
                const getCleanText = (node) => {
                    if (!node) return "";
                    const raw = node.innerText || node.textContent || "";
                    return raw.replace(OBF_RE, "").replace(SKEL_RE, " ").replace(/\\s+/g, " ").trim();
                };

                const articles = Array.from(document.querySelectorAll('div[role="article"]'));
                let mainPost = "";
                let author = "Auteur Facebook";

                if (articles.length > 0) {
                    const first = articles[0];
                    const authorEl = first.querySelector('[data-ad-rendering-role="profile_name"], h2, h3, h4, strong');
                    if (authorEl) author = getCleanText(authorEl) || author;

                    const msgEl = first.querySelector('div[data-ad-preview="message"], [data-ad-rendering-role="story_message"], [data-ad-comet-preview="message"]');
                    mainPost = msgEl ? getCleanText(msgEl) : getCleanText(first);
                }

                if (!mainPost) {
                    const msgGlobal = document.querySelector('div[data-ad-preview="message"], [data-ad-rendering-role="story_message"], div[role="main"]');
                    mainPost = getCleanText(msgGlobal || document.body);
                }

                const comments = [];
                const seen = new Set();
                for (let i = 1; i < articles.length && comments.length < maxComm; i++) {
                    const txt = getCleanText(articles[i]);
                    if (txt.length > 2 && !seen.has(txt)) {
                        seen.add(txt);
                        comments.push(txt.slice(0, 800));
                    }
                }

                if (comments.length === 0) {
                    const commNodes = Array.from(document.querySelectorAll('div[aria-label*="Commentaire de"], div[aria-label*="Comment by"]'));
                    for (const cn of commNodes) {
                        if (comments.length >= maxComm) break;
                        const txt = getCleanText(cn);
                        if (txt.length > 2 && !seen.has(txt)) {
                            seen.add(txt);
                            comments.push(txt.slice(0, 800));
                        }
                    }
                }

                return {
                    author: author,
                    main_post: mainPost.slice(0, 3500),
                    comments: comments
                };
            }""", max_comments)

            if stories:
                first_story = next(iter(stories.values()))
                if len(first_story.get("content", "")) > len(data.get("main_post", "")):
                    data["main_post"] = first_story["content"]
                if data.get("author") == "Auteur Facebook" and first_story.get("author"):
                    data["author"] = first_story["author"]

            return {
                "post_url": post_url,
                "author": _clean_text(data.get("author") or "Auteur Facebook"),
                "main_post": _clean_text(data.get("main_post") or ""),
                "comments_count": len(data.get("comments") or []),
                "comments": [_clean_text(c) for c in (data.get("comments") or [])],
            }

        return await self._with_page("READ_POST", action)

    async def analyze_video(self, video_url: str) -> Dict[str, Any]:
        """
        7. Exécute yt-dlp --dump-json --no-download en sous-processus asynchrone pour récupérer
        titre, description, durée, vues, likes, sous-titres, et complète avec facebook_read_post
        pour donner tout le contexte vidéo à l'IA sans charger la vidéo en RAM.
        """
        metadata: Dict[str, Any] = {
            "video_url": video_url,
            "title": None,
            "description": None,
            "uploader": None,
            "duration_seconds": None,
            "view_count": None,
            "like_count": None,
            "comment_count": None,
            "subtitles_available": [],
            "tags": [],
        }

        yt_dlp_bin = os.path.join(os.path.dirname(sys.executable), "yt-dlp")
        if not os.path.exists(yt_dlp_bin):
            yt_dlp_bin = "yt-dlp"

        try:
            proc = await asyncio.create_subprocess_exec(
                yt_dlp_bin,
                "--dump-json",
                "--no-download",
                "--no-warnings",
                video_url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=35.0)
            if stdout:
                first_line = stdout.decode("utf-8", errors="ignore").splitlines()[0]
                info = json.loads(first_line)
                metadata.update({
                    "title": info.get("title"),
                    "description": info.get("description"),
                    "uploader": info.get("uploader"),
                    "duration_seconds": info.get("duration"),
                    "view_count": info.get("view_count"),
                    "like_count": info.get("like_count"),
                    "comment_count": info.get("comment_count"),
                    "tags": info.get("tags") or [],
                    "subtitles_available": list((info.get("subtitles") or {}).keys()),
                    "yt_dlp_extracted": True,
                })
            else:
                metadata["yt_dlp_extracted"] = False
                if stderr:
                    metadata["yt_dlp_note"] = stderr.decode("utf-8", errors="ignore").strip()[:300]
        except Exception as e:
            metadata["yt_dlp_extracted"] = False
            metadata["yt_dlp_note"] = f"Extraction yt-dlp partielle : {e}"

        try:
            dom_ctx = await self.read_post_and_comments(video_url, max_comments=10)
            metadata["facebook_dom_context"] = dom_ctx
            if not metadata.get("description") and dom_ctx.get("main_post"):
                metadata["description"] = dom_ctx["main_post"]
            if not metadata.get("uploader") and dom_ctx.get("author"):
                metadata["uploader"] = dom_ctx["author"]
        except FacebookAuthError:
            raise
        except Exception as e:
            metadata["dom_error"] = str(e)

        return metadata


bridge = UniversalFacebookBridge()


# --- 6. SERVEUR MCP (FASTMCP : STDIO + SSE + STREAMABLE HTTP) ---

def _make_mcp() -> FastMCP:
    attempts: List[Dict[str, Any]] = []
    try:
        from mcp.server.transport_security import TransportSecuritySettings

        attempts.append({
            "transport_security": TransportSecuritySettings(enable_dns_rebinding_protection=False),
            "stateless_http": True,
            "json_response": True,
        })
        attempts.append({
            "transport_security": TransportSecuritySettings(enable_dns_rebinding_protection=False),
        })
    except ImportError:
        pass
    attempts.append({"stateless_http": True, "json_response": True})
    attempts.append({})
    for kwargs in attempts:
        try:
            return FastMCP("Sershbook Universal AI Hub", **kwargs)
        except TypeError:
            continue
    return FastMCP("Sershbook Universal AI Hub")


mcp_server = _make_mcp()


@mcp_server.tool()
async def facebook_search(query: str, category: str = "posts", limit: int = 5) -> str:
    """
    Recherche Facebook (top, posts, videos, pages, people) avec scroll progressif et dé-obfuscation DOM.
    Retourne un JSON structuré contenant les publications trouvées.
    """
    req = SearchRequest(query=query, category=category, limit=limit)
    results = await bridge.search(query=req.query, category=req.category, limit=req.limit)
    await bridge.log_action("MCP-Agent", "search", req.query, "success")
    return json.dumps(
        {"status": "success", "tool": "facebook_search", "count": len(results), "results": results},
        ensure_ascii=False,
    )


@mcp_server.tool()
async def facebook_feed(limit: int = 5) -> str:
    """
    Lit le fil d'actualité chronologique Facebook (https://www.facebook.com/?sk=h_chr).
    Retourne un JSON structuré avec les derniers posts du fil.
    """
    req = FeedRequest(limit=limit)
    results = await bridge.get_feed(limit=req.limit)
    await bridge.log_action("MCP-Agent", "feed", f"limit={req.limit}", "success")
    return json.dumps(
        {"status": "success", "tool": "facebook_feed", "count": len(results), "results": results},
        ensure_ascii=False,
    )


@mcp_server.tool()
async def facebook_publish(
    content: str,
    media_url: Optional[str] = None,
    media_base64: Optional[str] = None,
    media_filename: str = "media.mp4",
) -> str:
    """
    Publie un texte, une photo ou une vidéo sur Facebook.
    Si un média est fourni (URL ou Base64), le télécharge dans /tmp, l'injecte et le nettoie dans finally.
    """
    req = PublishRequest(
        content=content,
        media_url=media_url,
        media_base64=media_base64,
        media_filename=media_filename,
    )
    res = await bridge.publish(
        content=req.content,
        media_url=req.media_url,
        media_base64=req.media_base64,
        filename=req.media_filename,
    )
    await bridge.log_action("MCP-Agent", "publish", req.content[:200], "success")
    return json.dumps(
        {"status": "success", "tool": "facebook_publish", "data": res},
        ensure_ascii=False,
    )


@mcp_server.tool()
async def facebook_comment(post_url: str, comment_text: str) -> str:
    """
    Ouvre un post Facebook, cible la boîte de commentaire (FR/EN) et publie comment_text.
    """
    req = CommentRequest(post_url=post_url, comment_text=comment_text)
    res = await bridge.comment_post(post_url=req.post_url, comment_text=req.comment_text)
    await bridge.log_action("MCP-Agent", "comment", f"{req.post_url} -> {req.comment_text}", "success")
    return json.dumps(
        {"status": "success", "tool": "facebook_comment", "data": res},
        ensure_ascii=False,
    )


@mcp_server.tool()
async def facebook_like(post_url: str) -> str:
    """
    Ouvre un post Facebook et clique sur le bouton J'aime / Like.
    """
    req = LikeRequest(post_url=post_url)
    res = await bridge.like_post(post_url=req.post_url)
    await bridge.log_action("MCP-Agent", "like", req.post_url, "success")
    return json.dumps(
        {"status": "success", "tool": "facebook_like", "data": res},
        ensure_ascii=False,
    )


@mcp_server.tool()
async def facebook_read_post(post_url: str, max_comments: int = 10) -> str:
    """
    Extrait le texte intégral d'un post Facebook et la liste de ses commentaires.
    """
    req = ReadPostRequest(post_url=post_url, max_comments=max_comments)
    res = await bridge.read_post_and_comments(post_url=req.post_url, max_comments=req.max_comments)
    await bridge.log_action("MCP-Agent", "read_post", req.post_url, "success")
    return json.dumps(
        {"status": "success", "tool": "facebook_read_post", "data": res},
        ensure_ascii=False,
    )


@mcp_server.tool()
async def facebook_analyze_video(video_url: str) -> str:
    """
    Exécute yt-dlp --dump-json --no-download en sous-processus asynchrone et complète avec facebook_read_post
    pour fournir tout le contexte vidéo (titre, description, durée, vues, likes, sous-titres, commentaires) sans charger la vidéo en RAM.
    """
    req = VideoAnalyzeRequest(video_url=video_url)
    res = await bridge.analyze_video(video_url=req.video_url)
    await bridge.log_action("MCP-Agent", "analyze_video", req.video_url, "success")
    return json.dumps(
        {"status": "success", "tool": "facebook_analyze_video", "video_analysis": res},
        ensure_ascii=False,
    )


# --- 7. SÉCURITÉ ASGI & ROUTAGE MCP SSE / HTTP ---

class MCPAuthMiddleware:
    """
    Middleware ASGI pur protégeant /sse, /messages et /mcp sans casser le streaming Server-Sent Events.
    Accepte :
    - Authorization: Bearer <KEY>
    - Authorization: <KEY>
    - X-API-Key: <KEY>
    - Paramètre d'URL ?key=<KEY> ou ?api_key=<KEY>
    - session_id déjà authentifié lors du handshake /sse pour les requêtes POST sur /messages.
    """

    PROTECTED_PREFIXES = ("/sse", "/messages", "/mcp")
    MAX_SESSIONS = 500
    SESSION_RE = re.compile(rb"session_id=([0-9a-fA-F\-]{16,64})")

    def __init__(self, app):
        self.app = app
        self.sessions: Dict[str, bool] = {}

    def _remember_session(self, session_id: str):
        self.sessions[session_id] = True
        while len(self.sessions) > self.MAX_SESSIONS:
            self.sessions.pop(next(iter(self.sessions)))

    async def _deny(self, send):
        body = json.dumps({"detail": "Clé API invalide ou manquante."}).encode("utf-8")
        await send({
            "type": "http.response.start",
            "status": 403,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("utf-8")),
            ],
        })
        await send({"type": "http.response.body", "body": body})

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if not path.startswith(self.PROTECTED_PREFIXES):
            await self.app(scope, receive, send)
            return

        headers = {k.decode("latin1").lower(): v.decode("latin1") for k, v in scope.get("headers", [])}
        query = urllib.parse.parse_qs(scope.get("query_string", b"").decode("utf-8", errors="ignore"))
        url_key = (query.get("key") or query.get("api_key") or [None])[0]
        session_id = (query.get("session_id") or [None])[0]

        authorized = (
            _token_is_valid(headers.get("authorization"), headers.get("x-api-key"))
            or (url_key is not None and _token_is_valid(None, url_key))
            or (path.startswith("/messages") and session_id is not None and session_id in self.sessions)
        )
        if not authorized:
            await self._deny(send)
            return

        if path.startswith("/sse"):
            async def capturing_send(message):
                if message.get("type") == "http.response.body":
                    body_bytes = message.get("body", b"") or b""
                    for m in self.SESSION_RE.finditer(body_bytes):
                        self._remember_session(m.group(1).decode("utf-8", errors="ignore"))
                await send(message)

            await self.app(scope, receive, capturing_send)
            return

        await self.app(scope, receive, send)


class MCPRouter:
    """Route /mcp vers le transport HTTP streamable et /sse + /messages vers le transport SSE."""

    def __init__(self, sse_app, http_app=None):
        self.sse_app = sse_app
        self.http_app = http_app

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        if self.http_app is not None and (path == "/mcp" or path.startswith("/mcp/")):
            await self.http_app(scope, receive, send)
        else:
            await self.sse_app(scope, receive, send)


# --- 8. APPLICATION FASTAPI & ROUTES REST (OPENAPI 3.1.0) ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await bridge._init_db()
    except Exception as e:
        print(f"[HUB IA] Base MySQL injoignable au démarrage ({e}), nouvelle tentative à la première action.", file=sys.stderr)
        bridge.db_pool = None

    manager = None
    try:
        manager = getattr(mcp_server, "session_manager", None)
    except Exception:
        manager = None

    try:
        if manager is not None:
            async with manager.run():
                yield
        else:
            yield
    finally:
        await bridge.shutdown()
        if bridge.db_pool is not None:
            bridge.db_pool.close()
            await bridge.db_pool.wait_closed()
            bridge.db_pool = None


app = FastAPI(
    title="Sershbook Universal AI Hub",
    description=(
        "Passerelle d'automatisation Facebook universelle pilotée par Playwright, "
        "connectée à MySQL et exposant un serveur MCP (SSE / Stdio / HTTP) + API REST OpenAPI 3.1.0."
    ),
    version="3.1.0",
    lifespan=lifespan,
)
app.openapi_version = "3.1.0"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(MCPAuthMiddleware)


async def verify_universal_auth(
    auth: Optional[str] = Security(auth_header),
    x_api_key: Optional[str] = Security(x_api_key_header),
) -> bool:
    if not _token_is_valid(auth, x_api_key):
        raise HTTPException(status_code=403, detail="Clé API invalide ou manquante.")
    return True


def _http_error(e: Exception) -> HTTPException:
    if isinstance(e, FacebookAuthError):
        return HTTPException(status_code=401, detail=str(e))
    return HTTPException(status_code=500, detail=str(e))


@app.get("/health", summary="Vérifier l'état de santé du Hub", tags=["Système"])
async def api_health():
    db_ok = False
    try:
        await bridge._init_db()
        if bridge.db_pool is not None:
            async with bridge.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    await cur.fetchone()
                    db_ok = True
    except Exception:
        db_ok = False

    return {
        "status": "online",
        "service": "Sershbook Universal AI Hub",
        "mysql_connected": db_ok,
        "browser_connected": bridge.browser is not None and bridge.browser.is_connected(),
        "queue_locked": bridge.lock.locked(),
    }


@app.post(
    "/reload-cookies",
    summary="Recharger les cookies Facebook depuis MySQL",
    tags=["Système"],
    dependencies=[Depends(verify_universal_auth)],
)
async def api_reload_cookies():
    if not bridge.context:
        await bridge._launch_browser()
        count = len(await bridge.context.cookies()) if bridge.context else 0
    else:
        count = await bridge._reload_cookies_from_db()
    return {"status": "success", "cookies_loaded": count}


@app.post(
    "/search",
    summary="Rechercher sur Facebook (top, posts, videos, pages, people)",
    tags=["Facebook AI Tools"],
    dependencies=[Depends(verify_universal_auth)],
)
async def api_search(req: SearchRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        results = await bridge.search(req.query, req.category, req.limit)
        await bridge.log_action(x_ai_agent, "search", req.query, "success")
        return {"status": "success", "agent": x_ai_agent, "count": len(results), "results": results}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "search", req.query, "error")
        raise _http_error(e)


@app.get(
    "/feed",
    summary="Lire le fil d'actualité chronologique Facebook",
    tags=["Facebook AI Tools"],
    dependencies=[Depends(verify_universal_auth)],
)
async def api_feed_get(
    limit: int = Query(default=5, ge=1, le=20),
    x_ai_agent: Optional[str] = Header(default="Universal-AI"),
):
    try:
        results = await bridge.get_feed(limit)
        await bridge.log_action(x_ai_agent, "feed", f"limit={limit}", "success")
        return {"status": "success", "agent": x_ai_agent, "count": len(results), "results": results}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "feed", f"limit={limit}", "error")
        raise _http_error(e)


@app.post(
    "/feed",
    summary="Lire le fil d'actualité chronologique Facebook (POST)",
    tags=["Facebook AI Tools"],
    dependencies=[Depends(verify_universal_auth)],
)
async def api_feed_post(
    req: Optional[FeedRequest] = None,
    x_ai_agent: Optional[str] = Header(default="Universal-AI"),
):
    limit = req.limit if req else 5
    try:
        results = await bridge.get_feed(limit)
        await bridge.log_action(x_ai_agent, "feed", f"limit={limit}", "success")
        return {"status": "success", "agent": x_ai_agent, "count": len(results), "results": results}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "feed", f"limit={limit}", "error")
        raise _http_error(e)


@app.post(
    "/publish",
    summary="Publier un texte, une photo ou une vidéo sur Facebook",
    tags=["Facebook AI Tools"],
    dependencies=[Depends(verify_universal_auth)],
)
async def api_publish(req: PublishRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.publish(
            req.content,
            req.media_url,
            req.media_base64,
            req.media_filename or "media.mp4",
        )
        await bridge.log_action(x_ai_agent, "publish", req.content[:200], "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "publish", req.content[:200], "error")
        raise _http_error(e)


@app.post(
    "/comment",
    summary="Commenter une publication Facebook",
    tags=["Facebook AI Tools"],
    dependencies=[Depends(verify_universal_auth)],
)
async def api_comment(req: CommentRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.comment_post(req.post_url, req.comment_text)
        await bridge.log_action(x_ai_agent, "comment", f"{req.post_url} -> {req.comment_text}", "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "comment", req.post_url, "error")
        raise _http_error(e)


@app.post(
    "/like",
    summary="Liker une publication Facebook",
    tags=["Facebook AI Tools"],
    dependencies=[Depends(verify_universal_auth)],
)
async def api_like(req: LikeRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.like_post(req.post_url)
        await bridge.log_action(x_ai_agent, "like", req.post_url, "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "like", req.post_url, "error")
        raise _http_error(e)


@app.post(
    "/read-post",
    summary="Lire intégralement un post Facebook et ses commentaires",
    tags=["Facebook AI Tools"],
    dependencies=[Depends(verify_universal_auth)],
)
async def api_read_post(req: ReadPostRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.read_post_and_comments(req.post_url, req.max_comments)
        await bridge.log_action(x_ai_agent, "read_post", req.post_url, "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "read_post", req.post_url, "error")
        raise _http_error(e)


@app.post(
    "/analyze-video",
    summary="Analyser une vidéo Facebook (yt-dlp + contexte DOM)",
    tags=["Facebook AI Tools"],
    dependencies=[Depends(verify_universal_auth)],
)
async def api_analyze_video(req: VideoAnalyzeRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.analyze_video(req.video_url)
        await bridge.log_action(x_ai_agent, "analyze_video", req.video_url, "success")
        return {"status": "success", "agent": x_ai_agent, "video_analysis": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "analyze_video", req.video_url, "error")
        raise _http_error(e)


@app.get("/ai-tools/manifest", summary="Manifeste universel des 7 outils IA (MCP & OpenAPI 3.1.0)", tags=["Système"])
async def api_ai_manifest(request: Request):
    base_url = str(request.base_url).rstrip("/")
    return {
        "name": "Sershbook Universal AI Hub",
        "version": "3.1.0",
        "openapi_version": "3.1.0",
        "hub_url": base_url,
        "openapi_schema_url": f"{base_url}/openapi.json",
        "mcp_sse_url": f"{base_url}/sse",
        "mcp_messages_url": f"{base_url}/messages",
        "mcp_http_url": f"{base_url}/mcp",
        "universal_tools": [
            {
                "name": "facebook_search",
                "endpoint": f"{base_url}/search",
                "method": "POST",
                "parameters": SearchRequest.model_json_schema(),
            },
            {
                "name": "facebook_feed",
                "endpoint": f"{base_url}/feed",
                "method": "GET",
                "parameters": FeedRequest.model_json_schema(),
            },
            {
                "name": "facebook_publish",
                "endpoint": f"{base_url}/publish",
                "method": "POST",
                "parameters": PublishRequest.model_json_schema(),
            },
            {
                "name": "facebook_comment",
                "endpoint": f"{base_url}/comment",
                "method": "POST",
                "parameters": CommentRequest.model_json_schema(),
            },
            {
                "name": "facebook_like",
                "endpoint": f"{base_url}/like",
                "method": "POST",
                "parameters": LikeRequest.model_json_schema(),
            },
            {
                "name": "facebook_read_post",
                "endpoint": f"{base_url}/read-post",
                "method": "POST",
                "parameters": ReadPostRequest.model_json_schema(),
            },
            {
                "name": "facebook_analyze_video",
                "endpoint": f"{base_url}/analyze-video",
                "method": "POST",
                "parameters": VideoAnalyzeRequest.model_json_schema(),
            },
        ],
    }


# Montage du serveur MCP SSE (/sse et /messages) et HTTP Streamable (/mcp) tout en bas
_sse_asgi = mcp_server.sse_app()
try:
    _http_asgi = mcp_server.streamable_http_app()
except AttributeError:
    _http_asgi = None
app.mount("/", MCPRouter(_sse_asgi, _http_asgi))


if __name__ == "__main__":
    if "--stdio" in sys.argv:
        mcp_server.run(transport="stdio")
    else:
        import uvicorn

        port = int(os.getenv("PORT", "8090"))
        uvicorn.run(app, host="0.0.0.0", port=port)
