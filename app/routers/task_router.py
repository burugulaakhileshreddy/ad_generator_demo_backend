"""
Task Router

Workflow:
User enters URL
    ↓
Task created
    ↓
Reuse check (last 30 days)
    ↓
If reusable:
    Clone previous scraped/script/voice data
Else:
    Business core scraped
    Script generated
    Voices generated
    Image assets scraped in parallel
    Scraped data stored
    First ad variant created
    ↓
Frontend editor customizes assets
    ↓
User clicks Download Video
    ↓
Frontend sends customization payload
    ↓
Backend renders video
    ↓
Video stored under variant
    ↓
Download URL returned
"""

# ---------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------

import json
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Depends, Body, UploadFile, File
from sqlalchemy.orm import Session

from app.database.db import SessionLocal

# Models
from app.models.task_model import Task
from app.models.scraped_data_model import ScrapedData
from app.models.ad_variant_model import AdVariant
from app.models.ad_script_model import AdScript
from app.models.ad_voice_model import AdVoice
from app.models.ad_music_model import AdMusic
from app.models.ad_video_model import AdVideo
from app.models.uploaded_image_model import UploadedImage

# Schema
from app.schemas.task_schema import TaskCreate

# Services
from app.services.scraper_service import (
    scrape_website,
    scrape_business_core,
    scrape_image_assets
)
from app.services.script_service import generate_ad_script
from app.services.voice_service import generate_all_voices
from app.services.render.video_renderer import render_video
from app.services.upload_service import save_uploaded_image
from app.services.variant_generation_service import (
    create_system_generated_variant,
    create_custom_generated_variant
)
from app.services.task_reuse_service import (
    find_reusable_task,
    clone_reusable_task_data
)

# Router
router = APIRouter()


# ---------------------------------------------------------
# DATABASE DEPENDENCY
# ---------------------------------------------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def format_asset_reference(value):
    if not value:
        return ""

    value = str(value).replace("\\", "/").strip()

    if value.startswith("http://") or value.startswith("https://"):
        return value

    return f"/{value.lstrip('/')}"


def serialize_voices(voice_records):
    voices = []

    for v in voice_records:
        voices.append({
            "voice_name": v.voice_name,
            "audio": format_asset_reference(v.audio_path)
        })

    return voices


def serialize_music(db: Session):
    music_records = db.query(AdMusic).all()

    music = []

    for m in music_records:
        music.append({
            "music_name": m.music_name,
            "audio": format_asset_reference(m.music_path)
        })

    return music


def serialize_images(image_list):
    if not image_list:
        return []
    return [format_asset_reference(img) for img in image_list if img]


def get_latest_variant(task_id: int, db: Session):
    return db.query(AdVariant).filter(
        AdVariant.task_id == task_id
    ).order_by(
        AdVariant.id.desc()
    ).first()


def get_variant_for_task(task_id: int, variant_id: int, db: Session):
    return db.query(AdVariant).filter(
        AdVariant.id == variant_id,
        AdVariant.task_id == task_id
    ).first()


def get_scraped_data_for_task(task_id: int, db: Session):
    return db.query(ScrapedData).filter(
        ScrapedData.task_id == task_id
    ).first()


def get_latest_script_for_variant(variant_id: int, db: Session):
    return db.query(AdScript).filter(
        AdScript.variant_id == variant_id
    ).order_by(
        AdScript.id.desc()
    ).first()


def build_variant_assets_response(task: Task, scraped_data: ScrapedData, variant_record: AdVariant, db: Session):
    script_record = get_latest_script_for_variant(variant_record.id, db)

    voice_records = db.query(AdVoice).filter(
        AdVoice.variant_id == variant_record.id
    ).all()

    return {
        "task_id": task.id,
        "variant_id": variant_record.id,
        "url": task.url,
        "business_name": scraped_data.business_name if scraped_data else "",
        "logo": format_asset_reference(scraped_data.business_logo) if scraped_data else "",
        "script": script_record.script if script_record else "",
        "images": serialize_images(scraped_data.images if scraped_data and scraped_data.images else []),
        "voices": serialize_voices(voice_records),
        "music": serialize_music(db)
    }


# ---------------------------------------------------------
# CREATE TASK API
# ---------------------------------------------------------

@router.post("/tasks")
def create_task(task: TaskCreate, db: Session = Depends(get_db)):

    new_task = Task(
        url=task.url,
    )

    db.add(new_task)
    db.commit()
    db.refresh(new_task)

    # -----------------------------------------------------
    # REUSE CHECK
    # -----------------------------------------------------

    try:
        reusable_task = find_reusable_task(
            url=new_task.url,
            db=db,
            within_days=30,
            exclude_task_id=new_task.id
        )
    except Exception as e:
        print("Task reuse check failed:", e)
        reusable_task = None

    if reusable_task:
        try:
            clone_result = clone_reusable_task_data(
                source_task_id=reusable_task.id,
                new_task_id=new_task.id,
                db=db
            )

            if clone_result:
                variant_record = get_latest_variant(new_task.id, db)
                scraped_data = get_scraped_data_for_task(new_task.id, db)

                if variant_record and scraped_data:
                    response = build_variant_assets_response(
                        task=new_task,
                        scraped_data=scraped_data,
                        variant_record=variant_record,
                        db=db
                    )

                    response["reused"] = True
                    response["reused_from_task_id"] = reusable_task.id

                    print(
                        f"[TASK {new_task.id}] Reused assets from task "
                        f"{reusable_task.id} for variant {variant_record.id}"
                    )

                    return response

        except Exception as e:
            print("Task reuse clone failed, continuing with fresh generation:", e)
            db.rollback()

    # -----------------------------------------------------
    # FRESH GENERATION FLOW
    # -----------------------------------------------------

    try:
        core_result = scrape_business_core(new_task.url, new_task.id)
    except Exception as e:
        print("Business core scrape crashed:", e)
        return {
            "error": "Business core scrape crashed",
            "task_id": new_task.id,
            "url": new_task.url
        }

    if not core_result:
        return {
            "error": "Business core scraping failed",
            "task_id": new_task.id,
            "url": new_task.url
        }

    variant_record = AdVariant(
        task_id=new_task.id
    )

    db.add(variant_record)
    db.commit()
    db.refresh(variant_record)

    image_executor = ThreadPoolExecutor(max_workers=1)
    image_future = image_executor.submit(
        scrape_image_assets,
        new_task.url,
        new_task.id,
        core_result.get("top_links", [])
    )

    script_record = None

    try:
        script_text = generate_ad_script(
            core_result.get("business_name"),
            new_task.url,
            core_result.get("business_info")
        )

        script_record = AdScript(
            variant_id=variant_record.id,
            script=script_text
        )

        db.add(script_record)
        db.commit()
        db.refresh(script_record)

        generated_voices = generate_all_voices(
            script_text,
            new_task.id,
            variant_record.id
        )

        for voice in generated_voices:
            voice_record = AdVoice(
                variant_id=variant_record.id,
                voice_name=voice["voice_name"],
                audio_path=voice["audio_path"]
            )
            db.add(voice_record)

        db.commit()

    except Exception as e:
        print("Script or voice generation failed:", e)
        db.rollback()

    try:
        images = image_future.result()
    except Exception as e:
        print("Image scraping failed:", e)
        images = []
    finally:
        image_executor.shutdown(wait=False)

    scraped_data = ScrapedData(
        task_id=new_task.id,
        business_name=core_result.get("business_name"),
        business_logo=core_result.get("logo_url"),
        business_info=core_result.get("business_info"),
        images=images
    )

    db.add(scraped_data)
    db.commit()

    voice_records = db.query(AdVoice).filter(
        AdVoice.variant_id == variant_record.id
    ).all()

    voices = serialize_voices(voice_records)
    music = serialize_music(db)

    print(f"[TASK {new_task.id}] Assets ready for variant {variant_record.id}")

    return {
        "task_id": new_task.id,
        "variant_id": variant_record.id,
        "url": new_task.url,
        "business_name": core_result.get("business_name") or "",
        "logo": format_asset_reference(core_result.get("logo_url")) or "",
        "script": script_record.script if script_record else "",
        "images": serialize_images(images),
        "voices": voices,
        "music": music,
        "reused": False
    }


# ---------------------------------------------------------
# GET TASK STATUS API
# ---------------------------------------------------------

@router.get("/tasks/{task_id}")
def get_task(task_id: int, db: Session = Depends(get_db)):

    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        return {"error": "Task not found"}

    latest_variant = get_latest_variant(task_id, db)

    return {
        "task_id": task.id,
        "variant_id": latest_variant.id if latest_variant else None,
        "url": task.url,
        "created_at": task.created_at
    }


# ---------------------------------------------------------
# GET ALL VARIANTS FOR TASK
# ---------------------------------------------------------

@router.get("/tasks/{task_id}/variants")
def get_task_variants(task_id: int, db: Session = Depends(get_db)):

    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        return {"error": "Task not found"}

    variants = db.query(AdVariant).filter(
        AdVariant.task_id == task_id
    ).order_by(
        AdVariant.id.asc()
    ).all()

    latest_variant = get_latest_variant(task_id, db)

    variant_items = []
    for index, variant in enumerate(variants, start=1):
        variant_items.append({
            "variant_id": variant.id,
            "label": f"Ad {index}",
            "created_at": variant.created_at,
            "is_latest": True if latest_variant and latest_variant.id == variant.id else False
        })

    return {
        "task_id": task.id,
        "url": task.url,
        "variants": variant_items
    }


# ---------------------------------------------------------
# GET SINGLE VARIANT ASSETS
# ---------------------------------------------------------

@router.get("/tasks/{task_id}/variants/{variant_id}")
def get_task_variant_assets(
    task_id: int,
    variant_id: int,
    db: Session = Depends(get_db)
):
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        return {"error": "Task not found"}

    variant_record = get_variant_for_task(task_id, variant_id, db)

    if not variant_record:
        return {"error": "Variant not found for this task"}

    scraped_data = get_scraped_data_for_task(task_id, db)

    if not scraped_data:
        return {"error": "Scraped data not found for this task"}

    return build_variant_assets_response(task, scraped_data, variant_record, db)


# ---------------------------------------------------------
# GENERATE SYSTEM VARIANT
# ---------------------------------------------------------

@router.post("/tasks/{task_id}/variants/system-generate")
def generate_system_variant(
    task_id: int,
    db: Session = Depends(get_db)
):
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        return {"error": "Task not found"}

    result = create_system_generated_variant(task_id, db)
    return result


# ---------------------------------------------------------
# GENERATE CUSTOM VARIANT
# ---------------------------------------------------------

@router.post("/tasks/{task_id}/variants/custom-generate")
def generate_custom_variant(
    task_id: int,
    payload: dict = Body(...),
    db: Session = Depends(get_db)
):
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        return {"error": "Task not found"}

    user_prompt = (
        payload.get("prompt")
        or payload.get("user_prompt")
        or payload.get("message")
        or ""
    ).strip()

    if not user_prompt:
        return {"error": "Custom prompt is required"}

    result = create_custom_generated_variant(task_id, user_prompt, db)
    return result


# ---------------------------------------------------------
# REGENERATE VOICES API
# ---------------------------------------------------------

@router.post("/regenerate-voices/{task_id}")
def regenerate_voices(
    task_id: int,
    payload: dict = Body(...),
    db: Session = Depends(get_db)
):
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        return {"error": "Task not found"}

    variant_id = payload.get("variant_id")
    new_script = (payload.get("script") or "").strip()

    if not new_script:
        return {"error": "Script is required"}

    if variant_id:
        variant_record = get_variant_for_task(task_id, variant_id, db)
    else:
        variant_record = get_latest_variant(task_id, db)

    if not variant_record:
        return {"error": "Variant not found for this task"}

    script_record = db.query(AdScript).filter(
        AdScript.variant_id == variant_record.id
    ).order_by(
        AdScript.id.desc()
    ).first()

    if not script_record:
        script_record = AdScript(
            variant_id=variant_record.id,
            script=new_script
        )
        db.add(script_record)
        db.commit()
        db.refresh(script_record)
    else:
        script_record.script = new_script
        db.commit()
        db.refresh(script_record)

    old_voice_records = db.query(AdVoice).filter(
        AdVoice.variant_id == variant_record.id
    ).all()

    for old_voice in old_voice_records:
        db.delete(old_voice)

    db.commit()

    try:
        generated_voices = generate_all_voices(
            new_script,
            task_id,
            variant_record.id
        )

        for voice in generated_voices:
            voice_record = AdVoice(
                variant_id=variant_record.id,
                voice_name=voice["voice_name"],
                audio_path=voice["audio_path"]
            )
            db.add(voice_record)

        db.commit()

    except Exception as e:
        print("Voice regeneration failed:", e)
        db.rollback()
        return {
            "error": "Voice regeneration failed",
            "task_id": task_id,
            "variant_id": variant_record.id,
            "url": task.url
        }

    fresh_voice_records = db.query(AdVoice).filter(
        AdVoice.variant_id == variant_record.id
    ).all()

    return {
        "message": "Voices regenerated successfully",
        "task_id": task_id,
        "variant_id": variant_record.id,
        "url": task.url,
        "script": script_record.script,
        "voices": serialize_voices(fresh_voice_records)
    }


# ---------------------------------------------------------
# RENDER VIDEO API
# ---------------------------------------------------------

@router.post("/render-video/{task_id}")
def render_task_video(
    task_id: int,
    payload: dict = Body(...),
    db: Session = Depends(get_db)
):

    print("\n================ RENDER REQUEST START ================\n")
    print("task_id from URL:", task_id)
    print("payload received from frontend:")
    print(json.dumps(payload, indent=2))
    print("\n================ RENDER REQUEST END ==================\n")

    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        return {"error": "Task not found"}

    variant_id = payload.get("variant_id")

    if variant_id:
        variant_record = get_variant_for_task(task_id, variant_id, db)
    else:
        variant_record = get_latest_variant(task_id, db)

    if not variant_record:
        return {"error": "Variant not found for this task"}

    payload["variant_id"] = variant_record.id

    try:
        output_path = render_video(task_id, payload, db)
    except Exception as e:
        print("Render failed:", e)
        return {
            "error": "Video rendering failed",
            "task_id": task_id,
            "variant_id": variant_record.id,
            "url": task.url
        }

    latest_video = db.query(AdVideo).filter(
        AdVideo.variant_id == variant_record.id
    ).order_by(
        AdVideo.id.desc()
    ).first()

    if not latest_video:
        return {
            "error": "Rendered file not saved in database",
            "task_id": task_id,
            "variant_id": variant_record.id,
            "url": task.url
        }

    video_url = format_asset_reference(latest_video.video_path)

    return {
        "message": "Video rendered successfully",
        "task_id": task_id,
        "variant_id": variant_record.id,
        "url": task.url,
        "video_url": video_url
    }


# ---------------------------------------------------------
# IMAGE UPLOAD API
# ---------------------------------------------------------

@router.post("/upload-image/{task_id}/{variant_id}")
def upload_image(
    task_id: int,
    variant_id: int,
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):

    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        return {"error": "Task not found"}

    variant_record = get_variant_for_task(task_id, variant_id, db)

    if not variant_record:
        return {"error": "Variant not found for this task"}

    try:
        path = save_uploaded_image(file, task_id, variant_id)

        uploaded_image = UploadedImage(
            task_id=task_id,
            variant_id=variant_id,
            image_path=path
        )

        db.add(uploaded_image)
        db.commit()
        db.refresh(uploaded_image)

        return {
            "message": "Image uploaded",
            "task_id": task.id,
            "variant_id": variant_record.id,
            "url": task.url,
            "image_url": format_asset_reference(path),
            "uploaded_image_id": uploaded_image.id
        }

    except Exception as e:
        print("Upload failed:", e)
        db.rollback()
        return {
            "error": "Upload failed",
            "task_id": task_id,
            "variant_id": variant_id,
            "url": task.url
        }