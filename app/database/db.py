import os
from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:SQLpost&&@localhost:5432/demo_adds_db"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()


# Importing models so SQLAlchemy registers them
from app.models.task_model import Task
from app.models.scraped_data_model import ScrapedData
from app.models.ad_variant_model import AdVariant
from app.models.ad_script_model import AdScript
from app.models.ad_voice_model import AdVoice
from app.models.ad_music_model import AdMusic
from app.models.ad_video_model import AdVideo
from app.models.uploaded_image_model import UploadedImage