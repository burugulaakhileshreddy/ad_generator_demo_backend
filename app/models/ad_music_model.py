
from sqlalchemy import Column, Integer, String, DateTime

from datetime import datetime

from app.database.db import Base


class AdMusic(Base):

    __tablename__ = "ad_music"

    id = Column(Integer, primary_key=True, index=True)

    # Name shown in frontend
    music_name = Column(String, nullable=False)

    # Path to music file (relative path)
    music_path = Column(String, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow)