from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import sources, sinks, pipelines, cronjobs, system
from pathlib import Path
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

app = FastAPI(title="Dataflow API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Routers
app.include_router(system.router)
app.include_router(sources.router)
app.include_router(sinks.router)
app.include_router(pipelines.router)
app.include_router(cronjobs.router)

UI_DIST = Path(__file__).parent.parent.parent / "ui" / "dist"
 
# Only serve static files if the build exists (i.e. running in Docker)
if UI_DIST.exists():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=UI_DIST / "assets"), name="assets")
 
    # Serve favicon and any other root-level static files
    @app.get("/favicon.svg")
    async def favicon():
        return FileResponse(UI_DIST / "favicon.svg")
 
    # Catch-all: serve index.html for any non-API route
    # This makes React Router work correctly
    @app.get("/{full_path:path}")
    async def serve_react(full_path: str):
        index = UI_DIST / "index.html"
        return FileResponse(index)