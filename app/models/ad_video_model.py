from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.database.db import Base


class AdVideo(Base):
    __tablename__ = "ad_videos"

    id = Column(Integer, primary_key=True, index=True)

    variant_id = Column(
        Integer,
        ForeignKey("ad_variants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    video_path = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    variant = relationship("AdVariant", back_populates="ad_videos")