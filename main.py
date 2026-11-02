import asyncio
import base64
import json
import os
import tempfile
import urllib.parse
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import aiomysql
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from playwright.async_api import async_playwright
from pydantic import BaseModel, Field

# --- CONFIGURATION SÉCURITÉ & BASE DE DONNÉES ---
API_KEY = os.getenv("FB_BRIDGE_API_KEY", "sershell_secret_token_2026")

auth_header = APIKeyHeader(name="Authorization", auto_error=False)
x_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'mysql-sershbook.alwaysdata.net'),
    'user': os.getenv('DB_USER', 'sershbook'),
    'password': os.getenv('DB_PASS', 'azertyuiop1234567890@/'),
    'db': os.getenv('DB_NAME', 'sershbook_db'),
    'autocommit': True
}

# --- MODÈLES DE DONNÉES UNIVERSELS ---

class SearchRequest(BaseModel):
    query: str = Field(..., description="Mots-clés exacts à rechercher sur Facebook.")
    category: str = Field(
        default="posts",
        description="Catégorie de recherche : 'posts' (publications), 'videos' (vidéos), 'pages' (pages), 'people' (personnes)."
    )
    limit: int = Field(default=5, ge=1, le=15, description="Nombre de résultats à retourner à l'IA (1 à 15).")

class PublishRequest(BaseModel):
    content: str = Field(..., description="Texte complet de la publication (rédigé par l'IA).")
    media_url: Optional[str] = Field(
        default=None,
        description="Optionnel : URL publique (HTTP/HTTPS) ou chemin local d'une photo ou d'une vidéo à publier."
    )
    media_base64: Optional[str] = Field(
        default=None,
        description="Optionnel : Fichier photo/vidéo encodé en Base64 si l'IA envoie directement le binaire."
    )
    media_filename: Optional[str] = Field(
        default="media.mp4",
        description="Nom du fichier avec son extension (ex: 'image.jpg' ou 'video.mp4') lors de l'utilisation de media_base64."
    )

class CommentRequest(BaseModel):
    post_url: str = Field(..., description="URL complète du post ou de la vidéo Facebook où commenter.")
    comment_text: str = Field(..., description="Texte du commentaire ou de la réponse à publier.")

class LikeRequest(BaseModel):
    post_url: str = Field(..., description="URL complète du post ou de la vidéo Facebook à liker.")

class ReadPostRequest(BaseModel):
    post_url: str = Field(..., description="URL de la publication Facebook dont l'IA veut lire le contenu et les commentaires.")
    max_comments: int = Field(default=10, ge=1, le=30, description="Nombre maximum de commentaires à extraire.")

class VideoAnalyzeRequest(BaseModel):
    video_url: str = Field(..., description="URL de la vidéo Facebook (Reel, Watch, Post) ou externe à analyser par l'IA.")


# --- MOTEUR D'AUTOMATISATION MULTI-IA OPTIMISÉ POUR DOCKER ---

class UniversalFacebookBridge:
    def __init__(self):
        self.db_pool = None
        self.playwright = None
        self.browser = None
        self.context = None
        self.lock = asyncio.Lock()

    async def _route_intercept(self, route):
        if route.request.resource_type in ["media", "font", "stylesheet"]:
            await route.abort()
        else:
            await route.continue_()

    async def _init_db(self):
        if not self.db_pool:
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

        print("[HUB IA] Démarrage de Chromium dans le conteneur Docker...")
        self.playwright = await async_playwright().start()

        custom_env = os.environ.copy()
        custom_env["DBUS_SESSION_BUS_ADDRESS"] = "unix:path=/dev/null"
        custom_env["DISPLAY"] = ""

        # Note : executable_path est omis pour laisser Playwright utiliser le Chromium système de Docker
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            env=custom_env,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--renderer-process-limit=1",
                "--js-flags=--max-old-space-size=256",
                "--blink-settings=imagesEnabled=false",
                "--no-first-run",
                "--no-default-browser-check"
            ]
        )

        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
            viewport={"width": 414, "height": 896},
            locale="fr-FR"
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
        """)
        return page

    async def _reload_cookies_from_db(self) -> int:
        async with self.db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT cookies_json FROM fb_sessions WHERE session_name = %s", ('main_account',))
                result = await cur.fetchone()
                if result:
                    cookies = json.loads(result['cookies_json'])
                    await self.context.add_cookies(cookies)
                    print(f"[HUB IA] {len(cookies)} cookies réinjectés.")
                    return len(cookies)
                print("[ERREUR] Aucun cookie trouvé dans MySQL !")
                return 0

    async def log_action(self, ai_agent: str, action_type: str, payload: str, status: str):
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO fb_ai_logs (ai_agent, action_type, payload, status) VALUES (%s, %s, %s, %s)",
                        (ai_agent[:50], action_type[:50], payload[:2000], status[:20])
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

    async def _prepare_temp_media(self, media_url: Optional[str], media_base64: Optional[str], filename: str) -> Optional[str]:
        if not media_url and not media_base64:
            return None

        ext = os.path.splitext(filename)[1]
        if not ext and media_url:
            parsed_path = urllib.parse.urlparse(media_url).path
            ext = os.path.splitext(parsed_path)[1]
        if not ext:
            ext = ".mp4"

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=ext, dir="/tmp")
        temp_path = temp_file.name
        temp_file.close()

        try:
            if media_base64:
                clean_b64 = media_base64.split(",")[-1]
                with open(temp_path, "wb") as f:
                    f.write(base64.b64decode(clean_b64))
                return temp_path

            if media_url:
                if os.path.exists(media_url):
                    return media_url
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
        return await page.evaluate("""(maxItems) => {
            const articles = Array.from(document.querySelectorAll('div[role="article"], article, div[data-story-id]'));
            const results = [];
            const seenTexts = new Set();

            for (const el of articles) {
                if (results.length >= maxItems) break;

                const rawText = (el.textContent || "").replace(/\\s+/g, " ").trim();
                if (rawText.length < 15 || seenTexts.has(rawText.slice(0, 100))) continue;
                seenTexts.add(rawText.slice(0, 100));

                let postUrl = null;
                const links = Array.from(el.querySelectorAll('a[href]'));
                for (const a of links) {
                    const href = a.href;
                    if (href.includes('/posts/') || href.includes('/videos/') || href.includes('/reel/') || href.includes('story_fbid=') || href.includes('/story.php')) {
                        postUrl = href.split('?')[0];
                        if (href.includes('story_fbid=')) postUrl = href;
                        break;
                    }
                }

                let author = "Auteur Facebook";
                const heading = el.querySelector('h3, h4, strong, a');
                if (heading && heading.textContent) {
                    author = heading.textContent.replace(/\\s+/g, " ").trim();
                }

                results.push({
                    author: author,
                    post_url: postUrl,
                    content: rawText.slice(0, 1500)
                });
            }
            return results;
        }""", limit)

    # --- FONCTIONS MÉTIERS ---

    async def search(self, query: str, category: str = "posts", limit: int = 5) -> List[Dict[str, Any]]:
        async with self.lock:
            for attempt in range(3):
                page = None
                try:
                    page = await self.get_page()
                    encoded_q = urllib.parse.quote(query)
                    target_url = f"https://m.facebook.com/search/posts/?q={encoded_q}"

                    await page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(3)
                    await page.evaluate("window.scrollBy(0, 800)")
                    await asyncio.sleep(2)

                    posts = await self._extract_dom_posts(page, limit)

                    if not posts:
                        fallback_text = await page.evaluate("""() => {
                            const main = document.querySelector('div#root') || document.body;
                            return (main.textContent || "").replace(/\\s+/g, " ").trim().slice(0, 3000);
                        }""")
                        if fallback_text:
                            posts = [{"author": "Flux Facebook Mobile", "post_url": target_url, "content": fallback_text}]
                    return posts
                except Exception as e:
                    print(f"[RETRY SEARCH] Tentative {attempt+1}/3 : {e}")
                    await self._launch_browser()
                    if attempt == 2:
                        raise e
                finally:
                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass

    async def get_feed(self, limit: int = 5) -> List[Dict[str, Any]]:
        async with self.lock:
            for attempt in range(3):
                page = None
                try:
                    page = await self.get_page()
                    await page.goto("https://m.facebook.com/", wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(3)
                    await page.evaluate("window.scrollBy(0, 1000)")
                    await asyncio.sleep(2)
                    return await self._extract_dom_posts(page, limit)
                except Exception as e:
                    print(f"[RETRY FEED] Tentative {attempt+1}/3 : {e}")
                    await self._launch_browser()
                    if attempt == 2:
                        raise e
                finally:
                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass

    async def publish(self, content: str, media_url: Optional[str] = None, media_base64: Optional[str] = None, filename: str = "media.mp4") -> Dict[str, Any]:
        async with self.lock:
            for attempt in range(3):
                page = None
                temp_media_path = None
                try:
                    page = await self.get_page()
                    temp_media_path = await self._prepare_temp_media(media_url, media_base64, filename)

                    await page.goto("https://m.facebook.com/", wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(3)

                    composer_triggers = [
                        'textarea[name="xc_message"]',
                        'div[role="button"]:has-text("Que voulez-vous dire")',
                        'div[role="button"]:has-text("Exprimez-vous")',
                        'span:has-text("Que voulez-vous dire")'
                    ]
                    clicked = False
                    for selector in composer_triggers:
                        loc = page.locator(selector).first
                        if await loc.count() > 0:
                            await loc.click(force=True)
                            clicked = True
                            break

                    if not clicked:
                        raise RuntimeError("Bouton de création de post introuvable.")

                    await asyncio.sleep(2)

                    if temp_media_path:
                        file_inputs = page.locator('input[type="file"]')
                        if await file_inputs.count() > 0:
                            await file_inputs.first.set_input_files(temp_media_path)
                            is_video = temp_media_path.lower().endswith((".mp4", ".mov", ".avi", ".mkv", ".webm"))
                            await asyncio.sleep(10 if is_video else 4)

                    textbox = page.locator('textarea, div[role="textbox"][contenteditable="true"]').first
                    await textbox.click(force=True)
                    await textbox.fill(content)
                    await asyncio.sleep(2)

                    publish_btn = page.locator('button[name="submit"], div[aria-label="Publier"], div[aria-label="Post"]').first
                    await publish_btn.click(force=True)
                    await asyncio.sleep(5)

                    return {"published": True, "text_preview": content[:80], "media_attached": bool(temp_media_path)}
                except Exception as e:
                    print(f"[RETRY PUBLISH] Tentative {attempt+1}/3 : {e}")
                    await self._launch_browser()
                    if attempt == 2:
                        raise e
                finally:
                    if temp_media_path and temp_media_path.startswith("/tmp") and os.path.exists(temp_media_path):
                        try:
                            os.remove(temp_media_path)
                        except Exception:
                            pass
                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass

    async def comment_post(self, post_url: str, comment_text: str) -> Dict[str, Any]:
        async with self.lock:
            for attempt in range(3):
                page = None
                try:
                    page = await self.get_page()
                    mobile_url = post_url.replace("www.facebook.com", "m.facebook.com")
                    await page.goto(mobile_url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(3)

                    comment_boxes = [
                        'textarea[name="comment_text"]',
                        'div[aria-label*="Écrivez un commentaire"]',
                        'div[role="textbox"][contenteditable="true"]'
                    ]
                    target_box = None
                    for sel in comment_boxes:
                        loc = page.locator(sel).first
                        if await loc.count() > 0:
                            target_box = loc
                            break

                    if not target_box:
                        raise RuntimeError("Zone de commentaire introuvable.")

                    await target_box.click(force=True)
                    await target_box.fill(comment_text)
                    await asyncio.sleep(1)
                    
                    submit_btn = page.locator('button[type="submit"], input[type="submit"]').first
                    if await submit_btn.count() > 0:
                        await submit_btn.click(force=True)
                    else:
                        await page.keyboard.press("Enter")
                    await asyncio.sleep(4)

                    return {"commented": True, "post_url": post_url, "comment": comment_text}
                except Exception as e:
                    print(f"[RETRY COMMENT] Tentative {attempt+1}/3 : {e}")
                    await self._launch_browser()
                    if attempt == 2:
                        raise e
                finally:
                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass

    async def like_post(self, post_url: str) -> Dict[str, Any]:
        async with self.lock:
            for attempt in range(3):
                page = None
                try:
                    page = await self.get_page()
                    mobile_url = post_url.replace("www.facebook.com", "m.facebook.com")
                    await page.goto(mobile_url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(3)

                    like_selectors = [
                        'a:has-text("J’aime")',
                        'a:has-text("J\'aime")',
                        'div[aria-label="J’aime"][role="button"]'
                    ]
                    for sel in like_selectors:
                        loc = page.locator(sel).first
                        if await loc.count() > 0:
                            await loc.click(force=True)
                            await asyncio.sleep(2)
                            return {"liked": True, "post_url": post_url}

                    raise RuntimeError("Bouton J'aime introuvable ou déjà cliqué.")
                except Exception as e:
                    print(f"[RETRY LIKE] Tentative {attempt+1}/3 : {e}")
                    await self._launch_browser()
                    if attempt == 2:
                        raise e
                finally:
                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass

    async def read_post_and_comments(self, post_url: str, max_comments: int = 10) -> Dict[str, Any]:
        async with self.lock:
            for attempt in range(3):
                page = None
                try:
                    page = await self.get_page()
                    mobile_url = post_url.replace("www.facebook.com", "m.facebook.com")
                    await page.goto(mobile_url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(3)

                    data = await page.evaluate("""(maxComm) => {
                        const mainElem = document.querySelector('div#root') || document.body;
                        const mainText = (mainElem.textContent || "").replace(/\\s+/g, " ").trim();
                        return { main_post: mainText.slice(0, 2500), comments: [] };
                    }""", max_comments)
                    return {"post_url": post_url, **data}
                except Exception as e:
                    print(f"[RETRY READ] Tentative {attempt+1}/3 : {e}")
                    await self._launch_browser()
                    if attempt == 2:
                        raise e
                finally:
                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass

    async def analyze_video(self, video_url: str) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {"video_url": video_url}
        try:
            proc = await asyncio.create_subprocess_exec(
                "yt-dlp", "--dump-json", "--no-download", "--no-warnings", video_url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
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
                    "subtitles_available": list((info.get("subtitles") or {}).keys())
                })
        except Exception as e:
            metadata["yt_dlp_note"] = f"Extraction directe partielle : {e}"

        try:
            dom_context = await self.read_post_and_comments(video_url, max_comments=8)
            metadata["facebook_dom_context"] = dom_context
        except Exception as e:
            metadata["dom_error"] = str(e)
        return metadata


# --- INITIALISATION FASTAPI ---

bridge = UniversalFacebookBridge()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await bridge._init_db()
    yield
    await bridge.shutdown()

app = FastAPI(
    title="Universal Facebook AI Hub (Render Docker Ready)",
    description="API passerelle universelle multi-agents IA.",
    version="2.8.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def verify_universal_auth(
    auth: Optional[str] = Security(auth_header),
    x_api_key: Optional[str] = Security(x_api_key_header)
) -> str:
    token = None
    if auth:
        token = auth.replace("Bearer ", "").replace("bearer ", "").strip()
    elif x_api_key:
        token = x_api_key.strip()

    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Clé API invalide ou manquante.")
    return token


# --- ROUTES EXPOSÉES ---

@app.get("/health", summary="Vérifier l'état du hub", tags=["Système"])
async def api_health():
    return {
        "status": "online",
        "browser_connected": bridge.browser is not None and bridge.browser.is_connected(),
        "queue_locked": bridge.lock.locked(),
    }

@app.post("/reload-cookies", summary="Recharger les cookies depuis MySQL", tags=["Système"], dependencies=[Depends(verify_universal_auth)])
async def api_reload_cookies():
    count = await bridge._reload_cookies_from_db()
    return {"status": "success", "cookies_loaded": count}

@app.post("/search", summary="Rechercher sur Facebook", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_search(req: SearchRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        results = await bridge.search(req.query, req.category, req.limit)
        await bridge.log_action(x_ai_agent, "search", req.query, "success")
        return {"status": "success", "agent": x_ai_agent, "count": len(results), "results": results}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "search", req.query, "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/feed", summary="Lire le fil d'actualité", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_feed(limit: int = 5, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        results = await bridge.get_feed(limit)
        await bridge.log_action(x_ai_agent, "feed", f"limit={limit}", "success")
        return {"status": "success", "agent": x_ai_agent, "count": len(results), "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/publish", summary="Publier sur Facebook", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_publish(req: PublishRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.publish(req.content, req.media_url, req.media_base64, req.media_filename or "media.mp4")
        await bridge.log_action(x_ai_agent, "publish", req.content[:200], "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "publish", req.content[:200], "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/comment", summary="Commenter un post", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_comment(req: CommentRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.comment_post(req.post_url, req.comment_text)
        await bridge.log_action(x_ai_agent, "comment", f"{req.post_url} -> {req.comment_text}", "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "comment", req.post_url, "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/like", summary="Liker un post", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_like(req: LikeRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.like_post(req.post_url)
        await bridge.log_action(x_ai_agent, "like", req.post_url, "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "like", req.post_url, "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/read-post", summary="Lire un post complet", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_read_post(req: ReadPostRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.read_post_and_comments(req.post_url, req.max_comments)
        await bridge.log_action(x_ai_agent, "read_post", req.post_url, "success")
        return {"status": "success", "agent": x_ai_agent, "data": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/analyze-video", summary="Analyser une vidéo", tags=["Facebook AI Tools"], dependencies=[Depends(verify_universal_auth)])
async def api_analyze_video(req: VideoAnalyzeRequest, x_ai_agent: Optional[str] = Header(default="Universal-AI")):
    try:
        res = await bridge.analyze_video(req.video_url)
        await bridge.log_action(x_ai_agent, "analyze_video", req.video_url, "success")
        return {"status": "success", "agent": x_ai_agent, "video_analysis": res}
    except Exception as e:
        await bridge.log_action(x_ai_agent, "analyze_video", req.video_url, "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ai-tools/manifest", summary="Manifeste AI universel", tags=["Système"])
async def api_ai_manifest(request: Request):
    base_url = str(request.base_url).rstrip("/")
    return {
        "hub_url": base_url,
        "openapi_schema_url": f"{base_url}/openapi.json",
        "universal_tools": [
            {"name": "facebook_search", "endpoint": f"{base_url}/search", "method": "POST", "parameters": SearchRequest.model_json_schema()},
            {"name": "facebook_publish", "endpoint": f"{base_url}/publish", "method": "POST", "parameters": PublishRequest.model_json_schema()},
            {"name": "facebook_comment", "endpoint": f"{base_url}/comment", "method": "POST", "parameters": CommentRequest.model_json_schema()},
            {"name": "facebook_like", "endpoint": f"{base_url}/like", "method": "POST", "parameters": LikeRequest.model_json_schema()},
            {"name": "facebook_analyze_video", "endpoint": f"{base_url}/analyze-video", "method": "POST", "parameters": VideoAnalyzeRequest.model_json_schema()}
        ]
    }
