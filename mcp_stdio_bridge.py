#!/usr/bin/env python3
"""
Sershbook Universal AI Hub - Adaptateur MCP STDIO Léger
Permet à Claude Desktop, Claude CLI et Gemini CLI / Antigravity d'utiliser les 7 outils Facebook :
- Soit via l'API REST distante/locale (ex: https://sershbook.onrender.com ou http://127.0.0.1:8090)
- Soit directement via le moteur Playwright local (SERSHBOOK_MODE="engine").
"""

import builtins
import json
import os
import sys
from typing import Any, Dict, Optional

# Protection stricte du flux stdout pour JSON-RPC MCP
_original_print = builtins.print


def _stderr_print(*args, **kwargs):
    kwargs.setdefault("file", sys.stderr)
    return _original_print(*args, **kwargs)


builtins.print = _stderr_print

import httpx
from mcp.server.fastmcp import FastMCP

API_KEY = os.getenv("FB_BRIDGE_API_KEY", "sershell_secret_token_2026")
MODE = os.getenv("SERSHBOOK_MODE", "remote").lower().strip()
DEFAULT_URL = "http://127.0.0.1:8090" if MODE == "local" else "https://sershbook.onrender.com"
API_BASE_URL = os.getenv("SERSHBOOK_API_URL", DEFAULT_URL).rstrip("/")
HTTP_TIMEOUT = float(os.getenv("SERSHBOOK_HTTP_TIMEOUT", "120.0"))
AI_AGENT_NAME = os.getenv("SERSHBOOK_AI_AGENT", "MCP-Stdio-Bridge")

mcp = FastMCP("Sershbook Universal AI Hub (Stdio Bridge)")

_local_bridge = None


async def _get_local_bridge():
    global _local_bridge
    if _local_bridge is None:
        from index import bridge as idx_bridge
        _local_bridge = idx_bridge
    return _local_bridge


async def _request_hub(method: str, endpoint: str, payload: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Appelle l'API Sershbook (locale ou Render) avec authentification hybride."""
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "X-API-Key": API_KEY,
        "X-AI-Agent": AI_AGENT_NAME,
        "Content-Type": "application/json",
    }
    url = f"{API_BASE_URL}{endpoint}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        if method.upper() == "GET":
            resp = await client.get(url, headers=headers, params=params)
        else:
            resp = await client.post(url, headers=headers, json=payload or {})
        resp.raise_for_status()
        return resp.json()


@mcp.tool()
async def facebook_search(query: str, category: str = "posts", limit: int = 5) -> str:
    """
    Recherche des publications, vidéos, pages ou personnes sur Facebook avec scroll progressif et dé-obfuscation DOM.
    Catégories supportées : 'top', 'posts', 'videos', 'pages', 'people'.
    """
    if MODE == "engine":
        b = await _get_local_bridge()
        res = await b.search(query=query, category=category, limit=limit)
        await b.log_action(AI_AGENT_NAME, "search", query, "success")
        return json.dumps({"status": "success", "mode": "engine", "count": len(res), "results": res}, ensure_ascii=False)
    data = await _request_hub("POST", "/search", {"query": query, "category": category, "limit": limit})
    return json.dumps(data, ensure_ascii=False)


@mcp.tool()
async def facebook_feed(limit: int = 5) -> str:
    """
    Lit le fil d'actualité chronologique Facebook (https://www.facebook.com/?sk=h_chr) et retourne les posts extraits.
    """
    if MODE == "engine":
        b = await _get_local_bridge()
        res = await b.get_feed(limit=limit)
        await b.log_action(AI_AGENT_NAME, "feed", f"limit={limit}", "success")
        return json.dumps({"status": "success", "mode": "engine", "count": len(res), "results": res}, ensure_ascii=False)
    data = await _request_hub("GET", "/feed", params={"limit": limit})
    return json.dumps(data, ensure_ascii=False)


@mcp.tool()
async def facebook_publish(
    content: str,
    media_url: Optional[str] = None,
    media_base64: Optional[str] = None,
    media_filename: str = "media.mp4",
) -> str:
    """
    Publie un statut texte, une photo ou une vidéo sur Facebook.
    Si un média (URL ou Base64) est fourni, il est téléchargé dans /tmp, injecté et nettoyé automatiquement.
    """
    if MODE == "engine":
        b = await _get_local_bridge()
        res = await b.publish(
            content=content,
            media_url=media_url,
            media_base64=media_base64,
            filename=media_filename,
        )
        await b.log_action(AI_AGENT_NAME, "publish", content[:200], "success")
        return json.dumps({"status": "success", "mode": "engine", "data": res}, ensure_ascii=False)
    payload = {
        "content": content,
        "media_url": media_url,
        "media_base64": media_base64,
        "media_filename": media_filename,
    }
    data = await _request_hub("POST", "/publish", payload)
    return json.dumps(data, ensure_ascii=False)


@mcp.tool()
async def facebook_comment(post_url: str, comment_text: str) -> str:
    """
    Ouvre une publication Facebook et poste un commentaire sous celle-ci.
    """
    if MODE == "engine":
        b = await _get_local_bridge()
        res = await b.comment_post(post_url=post_url, comment_text=comment_text)
        await b.log_action(AI_AGENT_NAME, "comment", f"{post_url} -> {comment_text}", "success")
        return json.dumps({"status": "success", "mode": "engine", "data": res}, ensure_ascii=False)
    data = await _request_hub("POST", "/comment", {"post_url": post_url, "comment_text": comment_text})
    return json.dumps(data, ensure_ascii=False)


@mcp.tool()
async def facebook_like(post_url: str) -> str:
    """
    Ouvre une publication Facebook et clique sur le bouton J'aime / Like.
    """
    if MODE == "engine":
        b = await _get_local_bridge()
        res = await b.like_post(post_url=post_url)
        await b.log_action(AI_AGENT_NAME, "like", post_url, "success")
        return json.dumps({"status": "success", "mode": "engine", "data": res}, ensure_ascii=False)
    data = await _request_hub("POST", "/like", {"post_url": post_url})
    return json.dumps(data, ensure_ascii=False)


@mcp.tool()
async def facebook_read_post(post_url: str, max_comments: int = 10) -> str:
    """
    Extrait le texte intégral d'un post Facebook ainsi que la liste de ses commentaires.
    """
    if MODE == "engine":
        b = await _get_local_bridge()
        res = await b.read_post_and_comments(post_url=post_url, max_comments=max_comments)
        await b.log_action(AI_AGENT_NAME, "read_post", post_url, "success")
        return json.dumps({"status": "success", "mode": "engine", "data": res}, ensure_ascii=False)
    data = await _request_hub("POST", "/read-post", {"post_url": post_url, "max_comments": max_comments})
    return json.dumps(data, ensure_ascii=False)


@mcp.tool()
async def facebook_analyze_video(video_url: str) -> str:
    """
    Exécute yt-dlp (--dump-json --no-download) en sous-processus asynchrone pour extraire titre, description,
    durée, vues, likes, sous-titres, et complète avec le contexte DOM et commentaires du post Facebook.
    """
    if MODE == "engine":
        b = await _get_local_bridge()
        res = await b.analyze_video(video_url=video_url)
        await b.log_action(AI_AGENT_NAME, "analyze_video", video_url, "success")
        return json.dumps({"status": "success", "mode": "engine", "video_analysis": res}, ensure_ascii=False)
    data = await _request_hub("POST", "/analyze-video", {"video_url": video_url})
    return json.dumps(data, ensure_ascii=False)


if __name__ == "__main__":
    mcp.run(transport="stdio")
