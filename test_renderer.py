from app.database.db import SessionLocal
from app.services.render.video_renderer import render_video


db = SessionLocal()

render_video(10, db)
