from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import relationship

from app.database.db import Base


class ScrapedData(Base):
    __tablename__ = "scraped_data"

    id = Column(Integer, primary_key=True, index=True)

    task_id = Column(
        Integer,
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        unique=True,
    )

    business_name = Column(String)
    business_logo = Column(String)
    business_info = Column(Text)
    images = Column(JSON)

    scraped_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    task = relationship("Task", back_populates="scraped_data")