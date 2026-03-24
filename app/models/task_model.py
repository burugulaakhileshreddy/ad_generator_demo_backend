from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import relationship

from app.database.db import Base


class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # One task -> one scraped data row
    scraped_data = relationship(
        "ScrapedData",
        back_populates="task",
        cascade="all, delete-orphan",
        uselist=False,
        passive_deletes=True,
    )

    # One task -> many ad variants
    ad_variants = relationship(
        "AdVariant",
        back_populates="task",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )