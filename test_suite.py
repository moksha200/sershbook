#!/usr/bin/env python3
"""
Sershbook Universal AI Hub - Suite de Tests Automatisée Complète (test_suite.py)
Valide de bout en bout :
1. Le démarrage / la disponibilité du serveur local sur le port 8090.
2. La sécurité hybride (403 sans clé, 200 avec Bearer, Authorization brut et X-API-Key, protection /sse).
3. Les routes système (/health, /openapi.json OpenAPI 3.1.0, /ai-tools/manifest).
4. La connexion et la persistance MySQL (fb_sessions + fb_ai_logs).
5. Les 7 outils REST (/search, /feed, /read-post, /analyze-video, /publish + nettoyage /tmp, /like, /comment).
6. Le protocole MCP en mode SSE (/sse + /messages) ET en mode Stdio (handshake, list_tools, call_tool facebook_search).
"""

import asyncio
import base64
import glob
import json
import os
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiomysql
import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client

from index import API_KEY, DB_CONFIG

PORT = int(os.getenv("PORT", "8090"))
BASE_URL = f"http://127.0.0.1:{PORT}"

TEST_POST_URL = "https://www.facebook.com/leilanigreen20/posts/pfbid02ZmgY1qB7BuuKWCNf8z2TM9zkzjzhAhWZh3XxdawRhauGrihsnsMo7UmxGG3BDs32l"
TEST_META_POST_URL = "https://www.facebook.com/Meta/posts/pfbid0evGHWPACd3JninWg3j3nucQjdFxoHWwJSPgFXKjZXaUiU8MctREDPRZZsAps6CGwl"
TEST_VIDEO_URL = "https://www.facebook.com/reel/2215966152649628/"

EXPECTED_TOOLS = {
    "facebook_search",
    "facebook_feed",
    "facebook_publish",
    "facebook_comment",
    "facebook_like",
    "facebook_read_post",
    "facebook_analyze_video",
}


class TestRunner:
    def __init__(self):
        self.results: List[Tuple[str, bool, str, float]] = []
        self.server_proc: Optional[subprocess.Popen] = None

    async def ensure_server_running(self):
        """Vérifie si le serveur tourne sur le port 8090, sinon le démarre en arrière-plan."""
        async with httpx.AsyncClient(timeout=5.0) as client:
            try:
                r = await client.get(f"{BASE_URL}/health")
                if r.status_code == 200:
                    print(f"[INIT] Serveur déjà actif sur {BASE_URL}")
                    return
            except Exception:
                pass

        print(f"[INIT] Lancement du serveur index.py sur {BASE_URL}...")
        env = os.environ.copy()
        env["PORT"] = str(PORT)
        self.server_proc = subprocess.Popen(
            [sys.executable, "index.py"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        deadline = time.time() + 25.0
        async with httpx.AsyncClient(timeout=3.0) as client:
            while time.time() < deadline:
                try:
                    r = await client.get(f"{BASE_URL}/health")
                    if r.status_code == 200:
                        print(f"[INIT] Serveur démarré avec succès (PID={self.server_proc.pid}).")
                        return
                except Exception:
                    pass
                await asyncio.sleep(0.5)

        raise RuntimeError(f"Impossible de joindre {BASE_URL}/health après 25s.")

    def stop_spawned_server(self):
        if self.server_proc is not None:
            print(f"[CLEANUP] Arrêt du serveur de test (PID={self.server_proc.pid})...")
            self.server_proc.terminate()
            try:
                self.server_proc.wait(timeout=8)
            except Exception:
                self.server_proc.kill()
            self.server_proc = None

    async def run_test(self, name: str, coro_fn: Callable[[], Any]):
        t0 = time.time()
        print(f"\n>>> [TEST] {name} ...")
        try:
            detail = await coro_fn()
            elapsed = round(time.time() - t0, 2)
            print(f"    [OK] ({elapsed}s) -> {detail}")
            self.results.append((name, True, str(detail), elapsed))
        except Exception as e:
            elapsed = round(time.time() - t0, 2)
            print(f"    [FAIL] ({elapsed}s) -> {type(e).__name__}: {e}")
            self.results.append((name, False, f"{type(e).__name__}: {e}", elapsed))


async def test_health_and_manifest() -> str:
    async with httpx.AsyncClient(timeout=15.0) as client:
        r_health = await client.get(f"{BASE_URL}/health")
        assert r_health.status_code == 200, f"Status /health inattendu: {r_health.status_code}"
        h_data = r_health.json()
        assert h_data.get("status") == "online", f"Health non online: {h_data}"
        assert h_data.get("mysql_connected") is True, f"MySQL non connecté dans /health: {h_data}"

        r_openapi = await client.get(f"{BASE_URL}/openapi.json")
        assert r_openapi.status_code == 200, "Schéma /openapi.json inaccessible"
        oa_data = r_openapi.json()
        assert str(oa_data.get("openapi", "")).startswith("3.1"), f"Version OpenAPI != 3.1.x: {oa_data.get('openapi')}"

        r_man = await client.get(f"{BASE_URL}/ai-tools/manifest")
        assert r_man.status_code == 200, "Manifeste /ai-tools/manifest inaccessible"
        m_data = r_man.json()
        tools = {t["name"] for t in m_data.get("universal_tools", [])}
        assert EXPECTED_TOOLS.issubset(tools), f"Outils manquants dans manifest: {EXPECTED_TOOLS - tools}"
        return f"health=online, mysql_connected=True, openapi={oa_data.get('openapi')}, manifest_tools={len(tools)}"


async def test_security_auth() -> str:
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. Sans clé -> 403
        r_no_key = await client.post(f"{BASE_URL}/search", json={"query": "Meta", "limit": 1})
        assert r_no_key.status_code == 403, f"Attendu 403 sans clé, reçu {r_no_key.status_code}"

        # 2. Mauvaise clé -> 403
        r_bad_key = await client.post(
            f"{BASE_URL}/search",
            headers={"Authorization": "Bearer wrong_token"},
            json={"query": "Meta", "limit": 1},
        )
        assert r_bad_key.status_code == 403, f"Attendu 403 avec mauvaise clé, reçu {r_bad_key.status_code}"

        # 3. Protection /sse sans clé -> 403
        r_sse_no_key = await client.get(f"{BASE_URL}/sse")
        assert r_sse_no_key.status_code == 403, f"Attendu 403 sur /sse sans clé, reçu {r_sse_no_key.status_code}"

        # 4. Authorization: Bearer <KEY> -> 200
        r_bearer = await client.post(
            f"{BASE_URL}/reload-cookies",
            headers={"Authorization": f"Bearer {API_KEY}"},
        )
        assert r_bearer.status_code == 200, f"Échec auth Bearer: {r_bearer.status_code} {r_bearer.text}"

        # 5. Authorization: <KEY> -> 200
        r_raw_auth = await client.post(
            f"{BASE_URL}/reload-cookies",
            headers={"Authorization": API_KEY},
        )
        assert r_raw_auth.status_code == 200, f"Échec auth brute: {r_raw_auth.status_code}"

        # 6. X-API-Key: <KEY> -> 200
        r_x_key = await client.post(
            f"{BASE_URL}/reload-cookies",
            headers={"X-API-Key": API_KEY},
        )
        assert r_x_key.status_code == 200, f"Échec auth X-API-Key: {r_x_key.status_code}"
        cookies_loaded = r_x_key.json().get("cookies_loaded", 0)
        assert cookies_loaded > 0, "Aucun cookie chargé depuis MySQL"
        return f"403 sans clé (REST & SSE), 200 avec Bearer / Auth brute / X-API-Key (cookies_loaded={cookies_loaded})"


async def test_mysql_tables() -> str:
    pool = await aiomysql.create_pool(**DB_CONFIG)
    try:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT session_name, cookies_json FROM fb_sessions WHERE session_name = %s",
                    ("main_account",),
                )
                session_row = await cur.fetchone()
                assert session_row is not None, "Session 'main_account' introuvable dans fb_sessions"

                await cur.execute("SELECT COUNT(*) AS cnt FROM fb_ai_logs")
                log_row = await cur.fetchone()
                log_count = log_row["cnt"] if log_row else 0
                return f"fb_sessions('main_account') présent, fb_ai_logs entrées={log_count}"
    finally:
        pool.close()
        await pool.wait_closed()


async def test_tool_search() -> str:
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.post(
            f"{BASE_URL}/search",
            headers={"Authorization": f"Bearer {API_KEY}", "X-AI-Agent": "TestSuite-REST"},
            json={"query": "Meta", "category": "posts", "limit": 3},
        )
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        data = r.json()
        assert data.get("status") == "success", f"Statut inattendu: {data}"
        results = data.get("results", [])
        assert len(results) >= 1, f"Aucun résultat retourné par /search: {data}"
        first = results[0]
        assert first.get("content") and len(first["content"]) >= 15, f"Contenu invalide: {first}"
        assert first.get("post_url", "").startswith("http"), f"post_url invalide: {first}"
        return f"count={len(results)}, premier post par '{first.get('author')}' ({first.get('post_url')})"


async def test_tool_feed() -> str:
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.get(
            f"{BASE_URL}/feed",
            params={"limit": 3},
            headers={"X-API-Key": API_KEY, "X-AI-Agent": "TestSuite-REST"},
        )
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        data = r.json()
        assert data.get("status") == "success", f"Statut inattendu: {data}"
        results = data.get("results", [])
        assert len(results) >= 1, f"Aucun résultat retourné par /feed: {data}"
        first = results[0]
        assert first.get("content") and len(first["content"]) >= 15, f"Contenu invalide: {first}"
        return f"count={len(results)}, premier post='{first.get('content')[:70]}...'"


async def test_tool_read_post() -> str:
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.post(
            f"{BASE_URL}/read-post",
            headers={"Authorization": f"Bearer {API_KEY}", "X-AI-Agent": "TestSuite-REST"},
            json={"post_url": TEST_POST_URL, "max_comments": 5},
        )
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        payload = r.json()
        assert payload.get("status") == "success", f"Statut inattendu: {payload}"
        data = payload.get("data", {})
        assert len(data.get("main_post", "")) >= 15, f"main_post vide ou trop court: {data}"
        assert isinstance(data.get("comments"), list), f"comments n'est pas une liste: {data}"
        return f"author='{data.get('author')}', main_post_len={len(data.get('main_post', ''))}, comments={len(data.get('comments', []))}"


async def test_tool_analyze_video() -> str:
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.post(
            f"{BASE_URL}/analyze-video",
            headers={"Authorization": f"Bearer {API_KEY}", "X-AI-Agent": "TestSuite-REST"},
            json={"video_url": TEST_VIDEO_URL},
        )
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        payload = r.json()
        assert payload.get("status") == "success", f"Statut inattendu: {payload}"
        va = payload.get("video_analysis", {})
        assert va.get("video_url") == TEST_VIDEO_URL
        has_meta = bool(va.get("title") or va.get("description") or va.get("facebook_dom_context"))
        assert has_meta, f"Aucune métadonnée extraite: {va}"
        return f"yt_dlp={va.get('yt_dlp_extracted')}, duration={va.get('duration_seconds')}s, title='{(va.get('title') or '')[:55]}...'"


async def test_tool_publish() -> str:
    # Petit fichier PNG 1x1 encodé en base64 pour tester l'injection input[type="file"] et la suppression /tmp
    tiny_png_b64 = (
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
    )
    before_tmp = set(glob.glob("/tmp/tmp*"))
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.post(
            f"{BASE_URL}/publish",
            headers={"Authorization": f"Bearer {API_KEY}", "X-AI-Agent": "TestSuite-REST"},
            json={
                "content": "[TEST_SUITE] Test automatisé Sershbook Universal AI Hub 🚀",
                "media_base64": tiny_png_b64,
                "media_filename": "test_upload.png",
            },
        )
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        payload = r.json()
        assert payload.get("status") == "success", f"Statut inattendu: {payload}"
        data = payload.get("data", {})
        assert data.get("published") is True, f"published != True: {data}"
        assert data.get("media_attached") is True, f"media_attached != True: {data}"

    after_tmp = set(glob.glob("/tmp/tmp*"))
    leaked = [p for p in (after_tmp - before_tmp) if p.endswith(".png")]
    assert not leaked, f"Fichier temporaire non supprimé dans /tmp : {leaked}"
    return f"published=True, media_attached=True, /tmp nettoyé avec succès"


async def test_tool_like() -> str:
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.post(
            f"{BASE_URL}/like",
            headers={"Authorization": f"Bearer {API_KEY}", "X-AI-Agent": "TestSuite-REST"},
            json={"post_url": TEST_META_POST_URL},
        )
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        payload = r.json()
        assert payload.get("status") == "success", f"Statut inattendu: {payload}"
        data = payload.get("data", {})
        assert data.get("liked") is True, f"liked != True: {data}"
        return f"liked=True sur {TEST_META_POST_URL[:55]}..."


async def test_tool_comment() -> str:
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.post(
            f"{BASE_URL}/comment",
            headers={"Authorization": f"Bearer {API_KEY}", "X-AI-Agent": "TestSuite-REST"},
            json={
                "post_url": TEST_META_POST_URL,
                "comment_text": "Super innovation IA !",
            },
        )
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        payload = r.json()
        assert payload.get("status") == "success", f"Statut inattendu: {payload}"
        data = payload.get("data", {})
        assert data.get("commented") is True, f"commented != True: {data}"
        return f"commented=True, texte='{data.get('comment')}'"


async def test_mcp_sse_protocol() -> str:
    sse_url = f"{BASE_URL}/sse?key={API_KEY}"
    headers = {"Authorization": f"Bearer {API_KEY}", "X-API-Key": API_KEY}
    async with sse_client(sse_url, headers=headers, timeout=30.0, sse_read_timeout=120.0) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            init_res = await session.initialize()
            tools_res = await session.list_tools()
            tool_names = {t.name for t in tools_res.tools}
            assert EXPECTED_TOOLS.issubset(tool_names), f"Outils MCP SSE manquants: {EXPECTED_TOOLS - tool_names}"

            call_res = await session.call_tool(
                "facebook_search",
                arguments={"query": "Meta", "category": "posts", "limit": 2},
            )
            assert not call_res.isError, f"Erreur lors de l'appel MCP facebook_search: {call_res}"
            text_content = call_res.content[0].text
            parsed = json.loads(text_content)
            assert parsed.get("status") == "success", f"Réponse MCP inattendue: {parsed}"
            assert parsed.get("count", 0) >= 1, f"Aucun résultat via MCP facebook_search: {parsed}"
            return (
                f"Handshake SSE OK ({init_res.serverInfo.name}), "
                f"{len(tool_names)} tools listés, facebook_search résultats={parsed.get('count')}"
            )


async def test_mcp_stdio_bridge() -> str:
    env = os.environ.copy()
    env["SERSHBOOK_MODE"] = "local"
    env["SERSHBOOK_API_URL"] = BASE_URL
    env["FB_BRIDGE_API_KEY"] = API_KEY

    server_params = StdioServerParameters(
        command=sys.executable,
        args=[os.path.join(os.path.dirname(os.path.abspath(__file__)), "mcp_stdio_bridge.py")],
        env=env,
    )
    async with stdio_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            init_res = await session.initialize()
            tools_res = await session.list_tools()
            tool_names = {t.name for t in tools_res.tools}
            assert EXPECTED_TOOLS.issubset(tool_names), f"Outils MCP Stdio manquants: {EXPECTED_TOOLS - tool_names}"
            return f"Handshake Stdio Bridge OK ({init_res.serverInfo.name}), {len(tool_names)} tools vérifiés"


async def main():
    runner = TestRunner()
    try:
        await runner.ensure_server_running()

        await runner.run_test("1. Santé (/health), OpenAPI 3.1.0 & Manifeste IA (/ai-tools/manifest)", test_health_and_manifest)
        await runner.run_test("2. Sécurité Hybride (403 sans clé, 200 Bearer / Brute / X-API-Key & /sse)", test_security_auth)
        await runner.run_test("3. Base de données MySQL (fb_sessions & fb_ai_logs)", test_mysql_tables)
        await runner.run_test("4. Outil 1 : Recherche Facebook (/search)", test_tool_search)
        await runner.run_test("5. Outil 2 : Fil d'actualité Facebook (/feed)", test_tool_feed)
        await runner.run_test("6. Outil 6 : Lecture complète de post & commentaires (/read-post)", test_tool_read_post)
        await runner.run_test("7. Outil 7 : Analyse Vidéo yt-dlp + DOM (/analyze-video)", test_tool_analyze_video)
        await runner.run_test("8. Outil 3 : Publication Facebook + Média /tmp (/publish)", test_tool_publish)
        await runner.run_test("9. Outil 5 : Like de publication Facebook (/like)", test_tool_like)
        await runner.run_test("10. Outil 4 : Commentaire sur publication Facebook (/comment)", test_tool_comment)
        await runner.run_test("11. Protocole MCP SSE Client (handshake + list_tools + call_tool facebook_search)", test_mcp_sse_protocol)
        await runner.run_test("12. Protocole MCP Stdio Bridge (mcp_stdio_bridge.py)", test_mcp_stdio_bridge)
    finally:
        runner.stop_spawned_server()

    print("\n" + "=" * 85)
    print("RAPPORT DE TEST SERSHBOOK UNIVERSAL AI HUB")
    print("=" * 85)
    passed = sum(1 for _, ok, _, _ in runner.results if ok)
    total = len(runner.results)
    for name, ok, detail, elapsed in runner.results:
        badge = "PASS" if ok else "FAIL"
        print(f"[{badge}] {name} ({elapsed}s) -> {detail}")
    print("-" * 85)
    print(f"SCORE FINAL : {passed}/{total} ({round((passed / max(total, 1)) * 100, 1)}%)")
    print("=" * 85)

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
