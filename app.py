"""
BRVM Trading Desk — Main application entry point.
FastAPI backend serving REST API + static frontend.
"""
import logging
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from backend.db.schema import init_db
from backend.api.routes import router
from backend.jobs.scheduler import start_async_scheduler as start_scheduler, stop_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Suppress noisy warnings
import warnings
warnings.filterwarnings("ignore")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("Starting BRVM Trading Desk...")
    await init_db()
    logger.info("Database initialized")
    start_scheduler()
    yield
    stop_scheduler()
    logger.info("BRVM Trading Desk stopped")


app = FastAPI(
    title="BRVM Trading Desk",
    description="Personal trading desk for BRVM market",
    version="1.0.0",
    lifespan=lifespan,
)

# API routes
app.include_router(router)

# Serve frontend static files
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
if os.path.isdir(frontend_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(frontend_dir, "assets")), name="assets")


@app.get("/")
async def serve_index():
    return FileResponse(os.path.join(frontend_dir, "index.html"))


@app.get("/{path:path}")
async def serve_spa(path: str):
    """SPA fallback — serve index.html for all non-API routes."""
    file_path = os.path.join(frontend_dir, path)
    if os.path.isfile(file_path):
        return FileResponse(file_path)
    return FileResponse(os.path.join(frontend_dir, "index.html"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
