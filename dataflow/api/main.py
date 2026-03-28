from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import sources, sinks, pipelines, cronjobs, system

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
