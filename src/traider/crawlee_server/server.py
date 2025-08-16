from __future__ import annotations

"""FastAPI application exposing a simple screenshot API backed by Crawlee.

Start the server (e.g. via ``uvicorn traider.crawlee_server.server:app``)
and request a screenshot using::

    http://127.0.0.1:8000/scrape?url=https://www.google.com
"""

import asyncio
from uuid import uuid4

from fastapi import FastAPI, Request
from starlette.responses import HTMLResponse

import crawlee

from .crawler import lifespan

__all__ = ["app"]

# --------------------------------------------------------------------------------------
# Application set-up
# --------------------------------------------------------------------------------------

app = FastAPI(lifespan=lifespan, title="Traider Crawlee Server")


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    """Simple index page with instructions."""
    return (
        """<!DOCTYPE html>
<html>
  <body>
    <h1>Traider Crawlee Screenshot Server</h1>
    <p>To capture a screenshot, visit the <code>/scrape</code> endpoint with the
       <code>url</code> query parameter. For example:<br/>
       <a href='/scrape?url=https://www.google.com'>/scrape?url=https://www.google.com</a>
    </p>
  </body>
</html>"""
    )


@app.get("/scrape")
async def scrape_url(request: Request, url: str | None = None) -> dict[str, object]:
    """Queue the URL for processing by the crawler and return the result."""

    if not url:
        return {"url": "missing", "scrape_result": "no results"}

    # ------------------------------------------------------------------
    # Create a unique key for this request so we can correlate the async
    # crawler result with the HTTP request.
    # ------------------------------------------------------------------
    unique_key = str(uuid4())

    # Access the shared application state prepared by the lifespan handler.
    state = request.app.state  # type: ignore[attr-defined]

    # Ensure the lifespan has initialised properly.
    if not hasattr(state, "requests_to_results") or not hasattr(state, "crawler"):
        raise RuntimeError("Crawler not initialised. Lifespan startup may have failed.")

    # Register a Future in the shared result dict. The crawler will fulfil it.
    state.requests_to_results[unique_key] = asyncio.Future()

    # Add the request to the Crawlee queue.
    await state.crawler.add_requests([
        crawlee.Request.from_url(url, unique_key=unique_key)
    ])

    # Await the crawler result.
    result: dict[str, object] = await state.requests_to_results[unique_key]

    # Clean up state to avoid leaks.
    state.requests_to_results.pop(unique_key, None)

    return {"url": url, "scrape_result": result}
