"""FastAPI application entry point."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from illdashboard.config import settings
from illdashboard.copilot_service import shutdown_client
from illdashboard.database import engine
from illdashboard.models import Base
from illdashboard.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # Ensure upload directory exists
    Path(settings.UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    yield
    # Shutdown Copilot SDK client
    await shutdown_client()


app = FastAPI(
    title="Health Dashboard API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")

# Serve uploaded files
upload_path = Path(settings.UPLOAD_DIR)
upload_path.mkdir(parents=True, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=str(upload_path)), name="uploads")
