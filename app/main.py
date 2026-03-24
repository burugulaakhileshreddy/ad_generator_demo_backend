import os
from dotenv import load_dotenv

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.database.db import engine
from app.models.task_model import Base
from app.models.scraped_data_model import ScrapedData
from app.routers.task_router import router as task_router

load_dotenv()

app = FastAPI()

# Create tables
Base.metadata.create_all(bind=engine)

# -----------------------------
# CORS SETTINGS
# -----------------------------
default_origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000"
]

frontend_origins_env = os.getenv("FRONTEND_ORIGINS", "")
frontend_origins = [
    origin.strip()
    for origin in frontend_origins_env.split(",")
    if origin.strip()
]

allowed_origins = frontend_origins if frontend_origins else default_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register router
app.include_router(task_router)

# Ensure storage folder exists before mounting static files
os.makedirs("storage", exist_ok=True)
app.mount("/storage", StaticFiles(directory="storage"), name="storage")


@app.get("/")
def root():
    return {"message": "Backend is running"}