import asyncio
import base64
import hmac
import json
import os
import tempfile
import time
import urllib.parse
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiomysql
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from mcp.server.fastmcp import FastMCP
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from pydantic import BaseModel, Field


# --- CONFIGURATION (Variables d'environnement Render / Alwaysdata) ---

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
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
)
ATTEMPTS = int(os.getenv("FB_ATTEMPTS", "2"))
GOTO_TIMEOUT_MS = int(os.getenv("FB_GOTO_TIMEOUT_MS", "35000"))
LOW_MEMORY = os.getenv("LOW_MEMORY", "1") == "1"

auth_header = APIKeyHeader(name="Authorization", auto_error=False)
x_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _token_is_valid(auth: Optional[str], x_api_key: Optional[str]) -> bool:
    token = None
    if auth:
        token = auth.strip()
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
    elif x_api_key:
        token = x_api_key.strip()
    if not token:
        return False
    return hmac.compare_digest(token.encode(), API_KEY.encode())


# --- MODÈLES DE DONNÉES ---

class SearchRequest(BaseModel):
    query: str = Field(..., description="Mots-clés exacts à rechercher sur Facebook.")
    category: str = Field(
        default="posts",
        description="Catégorie : 'top' (populaire), 'posts' (publications), 'videos' (vidéos), 'pages' (pages), 'people' (personnes).",
    )
    limit: int = Field(default=5, ge=1, le=15, description="Nombre de résultats (1 à 15).")


class PublishRequest(BaseModel):
    content: str = Field(..., description="Texte complet de la publication.")
    media_url: Optional[str] = Field(default=None, description="URL directe (HTTP/HTTPS) d'une photo ou vidéo.")
    media_base64: Optional[str] = Field(default=None, description="Fichier média encodé en Base64.")
    media_filename: Optional[str] = Field(default="media.mp4", description="Nom du fichier (ex: image.jpg, video.mp4).")


class CommentRequest(BaseModel):
    post_url: str = Field(..., description="URL directe du post Facebook.")
    comment_text: str = Field(..., description="Texte du commentaire à publier.")


class LikeRequest(BaseModel):
    post_url: str = Field(..., description="URL directe du post Facebook à liker.")


class ReadPostRequest(BaseModel):
    post_url: str = Field(..., description="URL directe du post Facebook à lire.")
    max_comments: int = Field(default=10, ge=1, le=30, description="Nombre maximum de commentaires à extraire.")


class VideoAnalyzeRequest(BaseModel):
    video_url: str = Field(..., description="URL de la vidéo Facebook à analyser.")


class FacebookAuthError(RuntimeError):
    """Session Facebook invalide (login / checkpoint). Inutile de réessayer."""


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


def _normalize_cookies(raw: Any) -> List[Dict[str, Any]]:
    """Rend les cookies exportés d'un navigateur compatibles avec Playwright."""
    cookies = []
    for c in raw:
        c = dict(c)
        same_site = str(c.get("sameSite", "")).lower()
        if same_site:
            c["sameSite"] = SAMESITE_MAP.get(same_site, "Lax")
        else:
            c.pop("sameSite", None)
        if "expirationDate" in c and "expires" not in c:
            c["expires"] = c.pop("expirationDate")
        for junk in ("hostOnly", "session", "storeId", "id"):
            c.pop(junk, None)
        if c.get("expires") in (None, 0, -1):
            c.pop("expires", None)
        if "domain" not in c or not c["domain"]:
            c["domain"] = ".facebook.com"
        if "path" not in c or not c["path"]:
            c["path"] = "/"
        cookies.append(c)
    return cookies


# --- MOTEUR D'AUTOMATISATION ---

class UniversalFacebookBridge:
    def __init__(self):
        self.db_pool = None
        self.playwright = None
        self.browser = None
        self.context = None
        self.lock = asyncio.Lock()

    async def _route_intercept(self, route):
        if route.request.resource_type in ("media", "font", "image"):
            await route.abort()
        else:
            await route.continue_()

    async def _init_db(self):
        if self.db_pool:
            return
        print("[HUB IA] Initialisation de la connexion MySQL...")
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

    async def _launch_browser(self):
        await self.shutdown()
        await self._init_db()

        print("[HUB IA] Démarrage de Chromium...")
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
            "--no-first-run",
            "--no-default-browser-check",
        ]
        if LOW_MEMORY:
            args += ["--renderer-process-limit=1", "--js-flags=--max-old-space-size=256"]

        # Utilise le binaire local Alwaysdata s'il existe, sinon le Chromium standard de Render
        alwaysdata_chrome = "/home/sershbook/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome"
        launch_kwargs: Dict[str, Any] = {"headless": True, "env": custom_env, "args": args}
        if os.path.exists(alwaysdata_chrome):
            launch_kwargs["executable_path"] = alwaysdata_chrome

        self.browser = await self.playwright.chromium.launch(**launch_kwargs)

        self.context = await self.browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 800},
            locale="fr-FR",
        )

        await self._reload_cookies_from_db()

    async def get_page(self):
        if not self.browser or not self.browser.is_connected():
            await self._launch_browser()

        try:
            page = await self.context.new_page()
        except Exception:
            print("[HUB IA] Régénération du contexte navigateur...")
            await self._launch_browser()
            page = await self.context.new_page()

        await page.route("**/*", self._route_intercept)
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.navigator.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'languages', { get: () => ['fr-FR', 'fr', 'en-US', 'en'] });
        """)
        return page

    async def _reload_cookies_from_db(self) -> int:
        await self._init_db()
        async with self.db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT cookies_json FROM fb_sessions WHERE session_name = %s",
                    ("main_account",),
                )
                result = await cur.fetchone()

        if not result:
            print("[ERREUR] Aucun cookie trouvé dans MySQL !")
            return 0

        raw = result["cookies_json"]
        if isinstance(raw, (str, bytes)):
            raw = json.loads(raw)
        cookies = _normalize_cookies(raw)

        if self.context:
            await self.context.add_cookies(cookies)
        print(f"[HUB IA] {len(cookies)} cookies réinjectés.")
        return len(cookies)

    async def log_action(self, ai_agent: str, action_type: str, payload: str, status: str):
        try:
            await self._init_db()
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO fb_ai_logs (ai_agent, action_type, payload, status) VALUES (%s, %s, %s, %s)",
                        (ai_agent[:50], action_type[:50], payload[:2000], status[:20]),
                    )
        except Exception as e:
            print(f"[LOG ERREUR] {e}")

    async def shutdown(self):
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
        self.browser = None
        self.playwright = None
        self.context = None

    # --- Outils internes de navigation & extraction ---

    async def _check_authenticated(self, page):
        """Vérifie si Facebook affiche un mur de connexion ou un checkpoint."""
        current = page.url.lower()
        if "/login" in current or "checkpoint" in current or "recover" in current:
            raise FacebookAuthError(
                f"Session Facebook expirée ou bloquée (URL: {page.url}). "
                "Réinjecte des cookies frais dans MySQL."
            )
        has_login_form = await page.locator('input[name="email"], input[name="pass"]').count()
        if has_login_form > 0:
            raise FacebookAuthError(
                "Facebook affiche le formulaire de connexion (cookies expirés ou invalides). "
                "Réexporte tes cookies et lance /reload-cookies."
            )

    async def _goto(self, page, url: str, wait_selector: Optional[str] = None):
        """Navigation tolérante avec détection d'expiration de session."""
        await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
        await asyncio.sleep(3)
        await self._check_authenticated(page)

        if wait_selector:
            try:
                await page.wait_for_selector(wait_selector, timeout=12000)
            except PlaywrightTimeoutError:
                print(f"[HUB IA] Sélecteur {wait_selector!r} lent sur {url}, extraction alternative activée.")

    async def _with_page(self, label: str, action: Callable[[Any], Awaitable[Any]]):
        """Exécute `action(page)` sous verrou avec retry automatique."""
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
                    print(f"[RETRY {label}] Tentative {attempt + 1}/{ATTEMPTS} : {e}")
                    if attempt < ATTEMPTS - 1:
                        try:
                            await self._launch_browser()
                        except Exception as relaunch_error:
                            print(f"[RETRY {label}] Relance impossible : {relaunch_error}")
                finally:
                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass
            raise last_error if last_error else RuntimeError(f"{label} : échec")

    async def _prepare_temp_media(
        self, media_url: Optional[str], media_base64: Optional[str], filename: str
    ) -> Optional[str]:
        if not media_url and not media_base64:
            return None

        ext = os.path.splitext(filename or "")[1]
        if not ext and media_url:
            ext = os.path.splitext(urllib.parse.urlparse(media_url).path)[1]
        if not ext:
            ext = ".mp4"

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
        temp_path = temp_file.name
        temp_file.close()

        try:
            if media_base64:
                clean_b64 = media_base64.split(",")[-1]
                with open(temp_path, "wb") as f:
                    f.write(base64.b64decode(clean_b64))
                return temp_path

            if media_url and os.path.exists(media_url):
                return media_url

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
                os.remove(temp_path)
            raise RuntimeError(f"Échec préparation média : {e}")

    async def _extract_dom_posts(self, page, limit: int) -> List[Dict[str, Any]]:
        """Extraction multi-niveaux : supporte le DOM React Desktop, les FeedUnits et le HTML mbasic."""
        return await page.evaluate("""(maxItems) => {
            const getCleanText = (node) => {
                if (!node) return "";
                const clone = node.cloneNode(true);
                clone.querySelectorAll('script, style, noscript, svg, template, form').forEach(s => s.remove());
                return (clone.textContent || "").replace(/\\s+/g, " ").trim();
            };

            const extractPermalink = (container) => {
                const links = Array.from(container.querySelectorAll('a[href]'));
                for (const a of links) {
                    const href = a.href || "";
                    if (
                        href.includes('/posts/') ||
                        href.includes('/videos/') ||
                        href.includes('/reel/') ||
                        href.includes('story_fbid=') ||
                        href.includes('/permalink/') ||
                        href.includes('/groups/') && href.includes('/permalink/')
                    ) {
                        if (href.includes('story_fbid=')) return href;
                        return href.split('?')[0];
                    }
                }
                return null;
            };

            // Niveau 1 : Conteneurs de posts standards (Desktop React + mbasic)
            let candidates = Array.from(document.querySelectorAll(
                'div[role="article"], article, div[data-pagelet^="FeedUnit"], div[data-ft], div[role="feed"] > div, div[role="main"] div[data-ad-preview="message"]'
            ));

            // Niveau 2 : Si Facebook masque les conteneurs parents, on remonte depuis les blocs de texte dir="auto"
            if (candidates.length === 0) {
                const textBlocks = Array.from(document.querySelectorAll('div[dir="auto"], span[dir="auto"]'));
                const parentSet = new Set();
                for (const tb of textBlocks) {
                    const txt = (tb.textContent || "").trim();
                    if (txt.length > 45) {
                        const card = tb.closest('div[class]')?.parentElement?.parentElement || tb.parentElement;
                        if (card) parentSet.add(card);
                    }
                }
                candidates = Array.from(parentSet);
            }

            const results = [];
            const seenFingerprints = new Set();

            for (const el of candidates) {
                if (results.length >= maxItems) break;

                const rawText = getCleanText(el);
                if (rawText.length < 30) continue;

                // Évite les doublons (conteneur parent + sous-conteneur)
                const fingerprint = rawText.slice(0, 90).toLowerCase();
                if (seenFingerprints.has(fingerprint)) continue;

                let isDuplicateSubstring = false;
                for (const existing of seenFingerprints) {
                    if (existing.includes(fingerprint.slice(0, 50)) || fingerprint.includes(existing.slice(0, 50))) {
                        isDuplicateSubstring = true;
                        break;
                    }
                }
                if (isDuplicateSubstring) continue;
                seenFingerprints.add(fingerprint);

                let postUrl = extractPermalink(el);

                let author = "Utilisateur Facebook";
                const heading = el.querySelector('h2, h3, h4, strong, a[role="link"] span, a > strong');
                if (heading) {
                    const hText = getCleanText(heading);
                    if (hText && hText.length > 1 && hText.length < 80) {
                        author = hText;
                    }
                }

                results.push({
                    author: author,
                    post_url: postUrl || window.location.href,
                    content: rawText.slice(0, 1500)
                });
            }
            return results;
        }""", limit)

    async def _fallback_mbasic_extract(self, page, mbasic_url: str, limit: int) -> List[Dict[str, Any]]:
        """Secours infaillible via mbasic.facebook.com (HTML serveur pur sans blocage React)."""
        print(f"[HUB IA] Bascule automatique sur mbasic : {mbasic_url}")
        await self._goto(page, mbasic_url)
        await asyncio.sleep(2)
        posts = await self._extract_dom_posts(page, limit)
        if not posts:
            raw_body = await page.evaluate("""() => {
                const clone = document.body.cloneNode(true);
                clone.querySelectorAll('script, style, nav, header, footer').forEach(s => s.remove());
                return (clone.textContent || "").replace(/\\s+/g, " ").trim().slice(0, 2500);
            }""")
            if raw_body and len(raw_body) > 40:
                posts = [{
                    "author": "Flux Facebook (Mode HTML)",
                    "post_url": mbasic_url,
                    "content": raw_body
                }]
        return posts

    # --- Actions Facebook exposées aux IA ---

    async def search(self, query: str, category: str = "posts", limit: int = 5) -> List[Dict[str, Any]]:
        path = SEARCH_PATHS.get(category.lower(), "posts")
        encoded_q = urllib.parse.quote(query)
        target_url = f"https://www.facebook.com/search/{path}/?q={encoded_q}"

        async def action(page):
            await self._goto(page, target_url, wait_selector='div[role="main"]')
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 1200)")
                await asyncio.sleep(1.5)

            posts = await self._extract_dom_posts(page, limit)
            if not posts:
                mbasic_search = f"https://mbasic.facebook.com/search/posts/?q={encoded_q}"
                posts = await self._fallback_mbasic_extract(page, mbasic_search, limit)
            return posts

        return await self._with_page("SEARCH", action)

    async def get_feed(self, limit: int = 5) -> List[Dict[str, Any]]:
        async def action(page):
            await self._goto(page, "https://www.facebook.com/", wait_selector='div[role="main"]')
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 1400)")
                await asyncio.sleep(2)

            posts = await self._extract_dom_posts(page, limit)
            if not posts:
                posts = await self._fallback_mbasic_extract(page, "https://mbasic.facebook.com/home.php", limit)
            return posts

        return await self._with_page("FEED", action)

    async def publish(
        self,
        content: str,
        media_url: Optional[str] = None,
        media_base64: Optional[str] = None,
        filename: str = "media.mp4",
    ) -> Dict[str, Any]:
        async def action(page):
            temp_media_path = None
            try:
                temp_media_path = await self._prepare_temp_media(media_url, media_base64, filename)

                await self._goto(page, "https://www.facebook.com/", wait_selector='div[role="main"]')

                composer_triggers = [
                    'div[role="button"]:has-text("Que voulez-vous dire")',
                    'div[role="button"]:has-text("Exprimez-vous")',
                    'div[role="button"]:has-text("What\'s on your mind")',
                    'span:has-text("Que voulez-vous dire")',
                ]
                clicked = False
                for selector in composer_triggers:
                    loc = page.locator(selector).first
                    if await loc.count() > 0:
                        await loc.click(force=True)
                        clicked = True
                        break

                # Si la modale React Desktop ne s'ouvre pas et qu'il n'y a pas de vidéo lourde, secours mbasic
                if not clicked and not temp_media_path:
                    await self._goto(page, "https://mbasic.facebook.com/")
                    mbasic_box = page.locator('textarea[name="xc_message"]').first
                    if await mbasic_box.count() > 0:
                        await mbasic_box.fill(content)
                        submit_btn = page.locator('input[type="submit"][name="view_post"]').first
                        await submit_btn.click()
                        await asyncio.sleep(3)
                        return {"published": True, "mode": "mbasic", "text_preview": content[:80], "media_attached": False}
                    raise RuntimeError("Bouton de création de post introuvable sur Facebook.")

                await asyncio.sleep(2)

                if temp_media_path:
                    file_inputs = page.locator('input[type="file"]')
                    if await file_inputs.count() == 0:
                        media_btn = page.locator('div[aria-label*="Photo/vidéo"], div[aria-label*="Photo/video"]').first
                        if await media_btn.count() > 0:
                            await media_btn.click(force=True)
                            await asyncio.sleep(2)
                    await page.locator('input[type="file"]').first.set_input_files(temp_media_path)
                    is_video = temp_media_path.lower().endswith((".mp4", ".mov", ".avi", ".mkv", ".webm"))
                    await asyncio.sleep(10 if is_video else 4)

                textbox = page.locator(
                    'div[role="dialog"] div[role="textbox"][contenteditable="true"], '
                    'div[role="textbox"][contenteditable="true"]'
                ).first
                await textbox.click(force=True)
                await textbox.fill(content)
                await asyncio.sleep(2)

                publish_btn = page.locator(
                    'div[aria-label="Publier"], div[aria-label="Post"], div[role="button"]:has-text("Publier")'
                ).first
                await publish_btn.click(force=True)
                await asyncio.sleep(5)

                return {"published": True, "mode": "desktop", "text_preview": content[:80], "media_attached": bool(temp_media_path)}
            finally:
                if temp_media_path and os.path.exists(temp_media_path):
                    try:
                        os.remove(temp_media_path)
                    except Exception:
                        pass

        return await self._with_page("PUBLISH", action)

    async def comment_post(self, post_url: str, comment_text: str) -> Dict[str, Any]:
        async def action(page):
            await self._goto(page, post_url, wait_selector='div[role="main"]')
            await asyncio.sleep(2)

            comment_boxes = [
                'div[aria-label*="Écrivez un commentaire"]',
                'div[aria-label*="Write a comment"]',
                'div[aria-label*="Répondre"]',
                'div[role="textbox"][contenteditable="true"]',
            ]
            target_box = None
            for sel in comment_boxes:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    target_box = loc
                    break

            if not target_box:
                raise RuntimeError("Zone de commentaire introuvable sur cette publication.")

            await target_box.click(force=True)
            await target_box.fill(comment_text)
            await asyncio.sleep(1)
            await page.keyboard.press("Enter")
            await asyncio.sleep(4)

            return {"commented": True, "post_url": post_url, "comment": comment_text}

        return await self._with_page("COMMENT", action)

    async def like_post(self, post_url: str) -> Dict[str, Any]:
        async def action(page):
            await self._goto(page, post_url, wait_selector='div[role="main"]')
            await asyncio.sleep(2)

            like_selectors = [
                'div[aria-label="J’aime"][role="button"]',
                'div[aria-label="J\'aime"][role="button"]',
                'div[aria-label="Like"][role="button"]',
            ]
            for sel in like_selectors:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    await loc.click(force=True)
                    await asyncio.sleep(2)
                    return {"liked": True, "post_url": post_url}

            raise RuntimeError("Bouton J'aime introuvable ou publication déjà likée.")

        return await self._with_page("LIKE", action)

    async def read_post_and_comments(self, post_url: str, max_comments: int = 10) -> Dict[str, Any]:
        async def action(page):
            await self._goto(page, post_url, wait_selector='div[role="main"]')
            await asyncio.sleep(3)

            data = await page.evaluate("""(maxComm) => {
                const getCleanText = (node) => {
                    const clone = node.cloneNode(true);
                    clone.querySelectorAll('script, style, noscript, svg, template').forEach(s => s.remove());
                    return (clone.textContent || "").replace(/\\s+/g, " ").trim();
                };
                const articles = Array.from(document.querySelectorAll('div[role="article"], article, div[data-ft]'));
                let mainPost = articles.length > 0 ? getCleanText(articles[0]) : "";
                if (!mainPost) {
                    const mainEl = document.querySelector('div[role="main"]') || document.body;
                    mainPost = getCleanText(mainEl);
                }
                const comments = [];
                for (let i = 1; i < articles.length && comments.length < maxComm; i++) {
                    const txt = getCleanText(articles[i]);
                    if (txt.length > 2) comments.push(txt.slice(0, 600));
                }
                return { main_post: mainPost.slice(0, 2500), comments: comments };
            }""", max_comments)
            return {"post_url": post_url, **data}

        return await self._with_page("READ", action)

    async def analyze_video(self, video_url: str) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {"video_url": video_url}
        try:
            proc = await asyncio.create_subprocess_exec(
                "yt-dlp", "--dump-json", "--no-download", "--no-warnings", video_url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=35.0)
            if stdout:
                info = json.loads(stdout.decode("utf-8", errors="ignore").splitlines()[0])
                metadata.update({
                    "title": info.get("title"),
                    "description": info.get("description"),
                    "uploader": info.get("uploader"),
                    "duration_seconds": info.get("duration"),
                    "view_count": info.get("view_count"),
                    "like_count": info.get("like_count"),
                    "comment_count": info.get("comment_count"),
                    "tags": info.get("tags", []),
                    "subtitles_available": list((info.get("subtitles") or {}).keys()),
                })
        except Exception as e:
            metadata["yt_dlp_note"] = f"Extraction directe partielle : {e}"

        try:
            metadata["facebook_dom_context"] = await self.read_post_and_comments(video_url, max_comments=8)
        except Exception as e:
            metadata["dom_error"] = str(e)
        return metadata


bridge = UniversalFacebookBridge()


# --- INTÉGRATION MCP (CLAUDE / CLIENTS MCP SSE) ---

try:
    from mcp.server.transport_security import TransportSecuritySettings

    mcp_server = FastMCP(
        "Facebook Bridge MCP",
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
except (ImportError, TypeError):
    mcp_server = FastMCP("Facebook Bridge MCP")


@mcp_server.tool()
async def facebook_search(query: str, category: str = "posts", limit: int = 5) -> str:
    """Recherche des publications sur Facebook par mots-clés."""
    res = await bridge.search(query=query, category=category, limit=limit)
    return json.dumps(res, ensure_ascii=False)


@mcp_server.tool()
async def facebook_feed(limit: int = 5) -> str:
    """Lit les publications récentes du fil d'actualité Facebook."""
    res = await bridge.get_feed(limit=limit)
    return json.dumps(res, ensure_ascii=False)


@mcp_server.tool()
async def facebook_publish(content: str, media_url: Optional[str] = None) -> str:
    """Publie un message texte ou un média sur Facebook."""
    res = await bridge.publish(content=content, media_url=media_url)
    return json.dumps(res, ensure_ascii=False)


@mcp_server.tool()
async def facebook_comment(post_url: str, comment_text: str) -> str:
    """Publie un commentaire sous un post ou une vidéo Facebook."""
    res = await bridge.comment_post(post_url=post_url, comment_text=comment_text)
    return json.dumps(res, ensure_ascii=False)


@mcp_server.tool()
async def facebook_like(post_url: str) -> str:
    """Aime (Like) une publication Facebook spécifiée."""
    res = await bridge.like_post(post_url=post_url)
    return json.dumps(res, ensure_ascii=False)


@mcp_server.tool()
async def facebook_read_post(post_url: str, max_comments: int = 10) -> str:
    """Lit le contenu d'un post et extrait ses commentaires."""
    res = await bridge.read_post_and_comments(post_url=post_url, max_comments=max_comments)
    return json.dumps(res, ensure_ascii=False)


@mcp_server.tool()
async def facebook_analyze_video(video_url: str) -> str:
    """Analyse les métadonnées et le contenu d'une vidéo Facebook."""
    res = await bridge.analyze_video(video_url=video_url)
    return json.dumps(res, ensure_ascii=False)


class MCPAuthMiddleware:
    """Protège les chemins MCP (/sse et /messages) avec la même clé API que le REST."""

    PROTECTED_PREFIXES = ("/sse", "/messages")

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"].startswith(self.PROTECTED_PREFIXES):
            headers = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
            if not _token_is_valid(headers.get("authorization"), headers.get("x-api-key")):
                body = json.dumps({"detail": "Clé API invalide ou manquante."}).encode()
                await send({
                    "type": "http.response.start",
                    "status": 403,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode()),
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                return
        await self.app(scope, receive, send)


# --- FASTAPI ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    await bridge._init_db()
    yield
    await bridge.shutdown()


app = FastAPI(
    title="Universal Facebook AI Hub",
    description="API passerelle universelle multi-agents IA (Gemini, Claude, ChatGPT) avec support REST et MCP SSE.",
    version="3.2.0",
    lifespan=lifespan,
)

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


# --- ROUTES REST ---

@app.get("/health", summary="Vérifier l'état du hub", tags=["Système"])
async def api_health():
    return {
        "status": "online",
        "browser_connected": bridge.browser is not None and bridge.browser.is_connected(),
        "queue_locked": bridge.lock.locked(),
    }


@app.get("/debug", summary="Diagnostic navigateur", tags=["Système"], dependencies=[Depends(verify_universal_auth)])
async def api_debug():
    out: Dict[str, Any] = {}
    async with bridge.lock:
        page = None
        try:
            page = await bridge.get_page()
            out["cookies_in_context"] = len(await bridge.context.cookies())
            for name, url in [("desktop_fb", "https://www.facebook.com/"), ("mbasic_fb", "https://mbasic.facebook.com/")]:
                t = time.time()
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(4)
                    out[name] = {
                        "ok": True,
                        "secs": round(time.time() - t, 1),
                        "final_url": page.url,
                        "title": await page.title(),
                        "has_login_form": (await page.locator('input[name="email"]').count()) > 0,
                    }
                except Exception as e:
                    out[name] = {"ok": False, "secs": round(time.time() - t, 1), "error": str(e)[:200]}
        except Exception as e:
            out["fatal"] = str(e)[:300]
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass
    return out


@app.get("/inspect", summary="Diagnostic texte de la page", tags=["Système"], dependencies=[Depends(verify_universal_auth)])
async def api_inspect(url: str = "https://www.facebook.com/", wait: int = 12):
    wait = max(3, min(wait, 40))
    events: List[str] = []
    async with bridge.lock:
        page = None
        try:
            page = await bridge.get_page()
            page.on("crash", lambda *_: events.append("PAGE CRASH (mémoire ?)"))
            page.on("pageerror", lambda e: events.append(f"pageerror: {str(e)[:150]}"))
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(wait)
            info = await page.evaluate("""() => {
                const txt = (document.body && document.body.innerText) || "";
                const n = (sel) => document.querySelectorAll(sel).length;
                return {
                    text_len: txt.length,
                    text_head: txt.replace(/\\s+/g, " ").slice(0, 700),
                    html_len: document.documentElement.outerHTML.length,
                    counts: {
                        article: n('div[role="article"], article'),
                        feed_units: n('div[data-pagelet^="FeedUnit"], div[data-ft]'),
                        dir_auto: n('div[dir="auto"]'),
                        feed: n('div[role="feed"]'),
                        main: n('div[role="main"]'),
                        login_form: n('input[name="email"]')
                    }
                };
            }""")
            return {"final_url": page.url, "title": await page.title(), "events": events[:10], **info}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)[:300])
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass


@app.get("/screenshot", summary="Capture d'écran de ce que voit Chromium", tags=["Système"], dependencies=[Depends(verify_universal_auth)])
async def api_screenshot(url: str = "https://www.facebook.com/", wait: int = 10):
    wait = max(3, min(wait, 40))
    async with bridge.lock:
        page = None
        try:
            page = await bridge.get_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(wait)
            png = await page.screenshot(type="png")
            return Response(content=png, media_type="image/png", headers={"X-Final-URL": page.url})
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)[:300])
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass


@app.post("/reload-cookies", summary="Recharger les cookies depuis MySQL", tags=["Système"], dependencies=[Depends(verify_universal_auth)])
async def api_reload_cookies():
    if not bridge.context:
        await bridge._launch_browser()
        count = len(await bridge.context.cookies())
    else:
        count = await bridge._reload_cookies_from_db()
    return {"status": "success", "cookies_loaded": count}


def _http_error(e: Exception) -> HTTPException:
    code = 401 if isinstance(e, FacebookAuthError) else 500
    return HTTPException(status_code=code, detail=str(e))


@app.post("/search", summary="Rechercher sur Facebook", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_search(req: SearchRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        results = await bridge.search(req.query, req.category, req.limit)
        await bridge.log_action(x_ai_agent, "search", req.query, "success")
        return {"status": "success", "agent": x_ai_agent, "count": len(results), "results": results}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "search", req.query, "error")
        raise _http_error(e)


@app.get("/feed", summary="Lire le fil d'actualité", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_feed(limit: int = 5, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        results = await bridge.get_feed(limit)
        await bridge.log_action(x_ai_agent, "feed", f"limit={limit}", "success")
        return {"status": "success", "agent": x_ai_agent, "count": len(results), "results": results}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "feed", f"limit={limit}", "error")
        raise _http_error(e)


@app.post("/publish", summary="Publier sur Facebook", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_publish(req: PublishRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.publish(req.content, req.media_url, req.media_base64, req.media_filename or "media.mp4")
        await bridge.log_action(x_ai_agent, "publish", req.content[:200], "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "publish", req.content[:200], "error")
        raise _http_error(e)


@app.post("/comment", summary="Commenter un post", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_comment(req: CommentRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.comment_post(req.post_url, req.comment_text)
        await bridge.log_action(x_ai_agent, "comment", f"{req.post_url} -> {req.comment_text}", "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "comment", req.post_url, "error")
        raise _http_error(e)


@app.post("/like", summary="Liker un post", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_like(req: LikeRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.like_post(req.post_url)
        await bridge.log_action(x_ai_agent, "like", req.post_url, "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "like", req.post_url, "error")
        raise _http_error(e)


@app.post("/read-post", summary="Lire un post complet", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_read_post(req: ReadPostRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.read_post_and_comments(req.post_url, req.max_comments)
        await bridge.log_action(x_ai_agent, "read_post", req.post_url, "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "read_post", req.post_url, "error")
        raise _http_error(e)


@app.post("/analyze-video", summary="Analyser une vidéo", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_analyze_video(req: VideoAnalyzeRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.analyze_video(req.video_url)
        await bridge.log_action(x_ai_agent, "analyze_video", req.video_url, "success")
        return {"status": "success", "agent": x_ai_agent, "video_analysis": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "analyze_video", req.video_url, "error")
        raise _http_error(e)


@app.get("/ai-tools/manifest", summary="Manifeste AI universel", tags=["Système"])
async def api_ai_manifest(request: Request):
    base_url = str(request.base_url).rstrip("/")
    return {
        "hub_url": base_url,
        "openapi_schema_url": f"{base_url}/openapi.json",
        "mcp_sse_url": f"{base_url}/sse",
        "universal_tools": [
            {"name": "facebook_search", "endpoint": f"{base_url}/search", "method": "POST", "parameters": SearchRequest.model_json_schema()},
            {"name": "facebook_feed", "endpoint": f"{base_url}/feed", "method": "GET", "parameters": {"limit": "integer (1-15)"}},
            {"name": "facebook_publish", "endpoint": f"{base_url}/publish", "method": "POST", "parameters": PublishRequest.model_json_schema()},
            {"name": "facebook_comment", "endpoint": f"{base_url}/comment", "method": "POST", "parameters": CommentRequest.model_json_schema()},
            {"name": "facebook_like", "endpoint": f"{base_url}/like", "method": "POST", "parameters": LikeRequest.model_json_schema()},
            {"name": "facebook_read_post", "endpoint": f"{base_url}/read-post", "method": "POST", "parameters": ReadPostRequest.model_json_schema()},
            {"name": "facebook_analyze_video", "endpoint": f"{base_url}/analyze-video", "method": "POST", "parameters": VideoAnalyzeRequest.model_json_schema()},
        ],
    }


# IMPORTANT : le montage MCP reste tout en bas pour ne pas écraser les routes REST FastAPI.
app.mount("/", mcp_server.sse_app())
