from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer
from sqlalchemy.orm import relationship

from app.database.db import Base


class AdVariant(Base):
    __tablename__ = "ad_variants"

    id = Column(Integer, primary_key=True, index=True)

    task_id = Column(
        Integer,
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    task = relationship("Task", back_populates="ad_variants")

    ad_scripts = relationship(
        "AdScript",
        back_populates="variant",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    ad_voices = relationship(
        "AdVoice",
        back_populates="variant",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    ad_videos = relationship(
        "AdVideo",
        back_populates="variant",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )