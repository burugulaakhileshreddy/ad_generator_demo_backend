from datetime import datetime, timedelta
from urllib.parse import urlparse

from sqlalchemy.orm import Session

from app.models.task_model import Task
from app.models.scraped_data_model import ScrapedData
from app.models.ad_variant_model import AdVariant
from app.models.ad_script_model import AdScript
from app.models.ad_voice_model import AdVoice


# ---------------------------------------------------------
# URL MATCH NORMALIZATION
# ---------------------------------------------------------

def normalize_url_for_matching(url: str) -> str:
    if not url:
        return ""

    cleaned = str(url).strip().lower()

    if not cleaned:
        return ""

    if not cleaned.startswith(("http://", "https://")):
        cleaned = "https://" + cleaned

    parsed = urlparse(cleaned)

    host = (parsed.netloc or "").strip().lower()

    if host.startswith("www."):
        host = host[4:]

    return host


# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def get_latest_variant_for_task(task_id: int, db: Session):
    return db.query(AdVariant).filter(
        AdVariant.task_id == task_id
    ).order_by(
        AdVariant.id.desc()
    ).first()


def get_latest_script_for_variant(variant_id: int, db: Session):
    return db.query(AdScript).filter(
        AdScript.variant_id == variant_id
    ).order_by(
        AdScript.id.desc()
    ).first()


def get_scraped_data_for_task(task_id: int, db: Session):
    return db.query(ScrapedData).filter(
        ScrapedData.task_id == task_id
    ).first()


def is_task_reusable(task_id: int, db: Session) -> bool:
    scraped_data = get_scraped_data_for_task(task_id, db)

    if not scraped_data:
        return False

    if not scraped_data.images or len(scraped_data.images) == 0:
        return False

    variant_record = get_latest_variant_for_task(task_id, db)

    if not variant_record:
        return False

    script_record = get_latest_script_for_variant(variant_record.id, db)

    if not script_record or not script_record.script:
        return False

    voice_records = db.query(AdVoice).filter(
        AdVoice.variant_id == variant_record.id
    ).all()

    if not voice_records:
        return False

    return True


# ---------------------------------------------------------
# FIND REUSABLE TASK
# ---------------------------------------------------------

def find_reusable_task(url: str, db: Session, within_days: int = 30, exclude_task_id: int = None):
    normalized_target = normalize_url_for_matching(url)

    if not normalized_target:
        return None

    cutoff = datetime.utcnow() - timedelta(days=within_days)

    recent_tasks = db.query(Task).filter(
        Task.created_at >= cutoff
    ).order_by(
        Task.id.desc()
    ).all()

    for task in recent_tasks:
        if exclude_task_id and task.id == exclude_task_id:
            continue

        candidate_normalized = normalize_url_for_matching(task.url)

        if candidate_normalized != normalized_target:
            continue

        if is_task_reusable(task.id, db):
            return task

    return None


# ---------------------------------------------------------
# CLONE REUSED ASSETS TO NEW TASK
# ---------------------------------------------------------

def clone_reusable_task_data(source_task_id: int, new_task_id: int, db: Session):
    source_scraped_data = get_scraped_data_for_task(source_task_id, db)

    if not source_scraped_data:
        return None

    source_variant = get_latest_variant_for_task(source_task_id, db)

    if not source_variant:
        return None

    source_script = get_latest_script_for_variant(source_variant.id, db)

    if not source_script:
        return None

    source_voices = db.query(AdVoice).filter(
        AdVoice.variant_id == source_variant.id
    ).all()

    if not source_voices:
        return None

    new_variant = AdVariant(
        task_id=new_task_id
    )
    db.add(new_variant)
    db.commit()
    db.refresh(new_variant)

    new_scraped_data = ScrapedData(
        task_id=new_task_id,
        business_name=source_scraped_data.business_name,
        business_logo=source_scraped_data.business_logo,
        business_info=source_scraped_data.business_info,
        images=source_scraped_data.images
    )
    db.add(new_scraped_data)
    db.commit()
    db.refresh(new_scraped_data)

    new_script = AdScript(
        variant_id=new_variant.id,
        script=source_script.script
    )
    db.add(new_script)
    db.commit()
    db.refresh(new_script)

    for voice in source_voices:
        new_voice = AdVoice(
            variant_id=new_variant.id,
            voice_name=voice.voice_name,
            audio_path=voice.audio_path
        )
        db.add(new_voice)

    db.commit()

    return {
        "variant_id": new_variant.id,
        "scraped_data_id": new_scraped_data.id,
        "script_id": new_script.id,
        "reused_from_task_id": source_task_id
    }