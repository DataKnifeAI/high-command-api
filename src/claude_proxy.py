"""
Claude API proxy - forwards /claude/* to Anthropic API with API key from config.
The key is read from CLAUDE_API_KEY env (typically from a k8s secret).
"""
import logging
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import Response
import httpx
from src.config import Config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/claude", tags=["Claude"])
ANTHROPIC_BASE = "https://api.anthropic.com"


@router.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_to_anthropic(request: Request, path: str):
    """Proxy requests to Anthropic API, adding API key from server config."""
    api_key = Config.CLAUDE_API_KEY
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="Claude API key not configured. Set CLAUDE_API_KEY in environment.",
        )

    # Map /claude/messages -> /v1/messages
    anthropic_path = f"/v1/{path}" if path else "/v1"
    url = f"{ANTHROPIC_BASE}{anthropic_path}"

    # Forward headers, ensure API key is set (server key takes precedence)
    headers = dict(request.headers)
    headers.pop("host", None)
    headers["x-api-key"] = api_key
    headers["anthropic-dangerous-direct-browser-access"] = "true"

    try:
        body = await request.body()
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.request(
                method=request.method,
                url=url,
                headers=headers,
                content=body,
            )
        # Forward only safe headers (exclude transfer-encoding, connection, etc.)
        forward_headers = {
            k: v for k, v in response.headers.items()
            if k.lower() not in ("transfer-encoding", "connection", "content-encoding")
        }
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=forward_headers,
        )
    except httpx.HTTPError as e:
        logger.error(f"Claude proxy error: {e}")
        raise HTTPException(status_code=502, detail=f"Upstream error: {str(e)}")
