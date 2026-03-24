import os
from typing import Optional, List

from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy.orm import Session

from app.models.task_model import Task
from app.models.scraped_data_model import ScrapedData
from app.models.ad_variant_model import AdVariant
from app.models.ad_script_model import AdScript
from app.models.ad_voice_model import AdVoice
from app.models.ad_music_model import AdMusic

from app.services.voice_service import generate_all_voices

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def format_asset_reference(value: str) -> str:
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


def get_task_with_scraped_data(task_id: int, db: Session):
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        return None, None

    scraped_data = db.query(ScrapedData).filter(
        ScrapedData.task_id == task_id
    ).first()

    return task, scraped_data


def get_existing_scripts_for_task(task_id: int, db: Session) -> List[str]:
    script_rows = (
        db.query(AdScript)
        .join(AdVariant, AdScript.variant_id == AdVariant.id)
        .filter(AdVariant.task_id == task_id)
        .order_by(AdScript.id.asc())
        .all()
    )

    scripts = []

    for row in script_rows:
        if row.script and row.script.strip():
            scripts.append(row.script.strip())

    return scripts


def build_existing_scripts_text(existing_scripts: List[str]) -> str:
    if not existing_scripts:
        return "No previous ad scripts exist for this website."

    parts = []

    for i, script in enumerate(existing_scripts, start=1):
        parts.append(f"Ad Script {i}:\n{script}")

    return "\n\n".join(parts)


# ---------------------------------------------------------
# PROMPT BUILDERS
# ---------------------------------------------------------

def build_system_variant_prompt(
    business_name: str,
    url: str,
    business_info: str,
    existing_scripts: List[str]
) -> str:
    previous_scripts_text = build_existing_scripts_text(existing_scripts)

    prompt = f"""
You are a professional advertising copywriter creating premium brand commercials.

Create a NEW and UNIQUE advertisement voiceover for this business.

Business Name: {business_name}
Website: {url}

Business information:
{business_info}

Existing ad scripts already created for this same business:
{previous_scripts_text}

Requirements:
- Create a fresh ad script that is clearly different from the existing scripts.
- Do not repeat wording, structure, opening, or closing from earlier scripts.
- The ad must be a continuous voiceover narration.
- Length should be suitable for a 30-32 second voiceover.
- The tone must be premium, cinematic, and engaging.
- Focus on the brand, products, and benefits.
- Do NOT include stage directions.
- Do NOT include visuals.
- Do NOT include scene descriptions.
- Only return the narration text.

Write the advertisement like a real commercial voiceover.
"""

    return prompt


def build_custom_variant_prompt(
    business_name: str,
    url: str,
    business_info: str,
    existing_scripts: List[str],
    user_prompt: str
) -> str:
    previous_scripts_text = build_existing_scripts_text(existing_scripts)

    prompt = f"""
You are a professional advertising copywriter creating premium brand commercials.

Create a NEW and UNIQUE advertisement voiceover for this business based on the user's custom requirement.

Business Name: {business_name}
Website: {url}

Business information:
{business_info}

Existing ad scripts already created for this same business:
{previous_scripts_text}

User custom requirement:
{user_prompt}

Requirements:
- Follow the user's requirement closely.
- Create a fresh ad script that is clearly different from the existing scripts.
- Do not repeat wording, structure, opening, or closing from earlier scripts.
- The ad must be a continuous voiceover narration.
- Length should be suitable for a 30-32 second voiceover.
- The tone should match the user's intent while still sounding polished and natural.
- Focus on the brand, products, and benefits.
- Do NOT include stage directions.
- Do NOT include visuals.
- Do NOT include scene descriptions.
- Only return the narration text.

Write the advertisement like a real commercial voiceover.
"""

    return prompt


# ---------------------------------------------------------
# SCRIPT GENERATION HELPERS
# ---------------------------------------------------------

def generate_system_variant_script(
    business_name: str,
    url: str,
    business_info: str,
    existing_scripts: List[str]
) -> str:
    prompt = build_system_variant_prompt(
        business_name=business_name,
        url=url,
        business_info=business_info,
        existing_scripts=existing_scripts
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.9,
        messages=[
            {
                "role": "system",
                "content": "You are an expert advertising copywriter who creates premium commercial scripts."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    script = (response.choices[0].message.content or "").strip()

    if not script:
        raise Exception("Empty script received from OpenAI")

    return script


def generate_custom_variant_script(
    business_name: str,
    url: str,
    business_info: str,
    existing_scripts: List[str],
    user_prompt: str
) -> str:
    prompt = build_custom_variant_prompt(
        business_name=business_name,
        url=url,
        business_info=business_info,
        existing_scripts=existing_scripts,
        user_prompt=user_prompt
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.9,
        messages=[
            {
                "role": "system",
                "content": "You are an expert advertising copywriter who creates premium commercial scripts."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    script = (response.choices[0].message.content or "").strip()

    if not script:
        raise Exception("Empty script received from OpenAI")

    return script


# ---------------------------------------------------------
# MAIN FLOWS
# ---------------------------------------------------------

def create_system_generated_variant(task_id: int, db: Session):
    task, scraped_data = get_task_with_scraped_data(task_id, db)

    if not task:
        return {"error": "Task not found"}

    if not scraped_data:
        return {"error": "Scraped data not found for this task"}

    existing_scripts = get_existing_scripts_for_task(task_id, db)

    try:
        script_text = generate_system_variant_script(
            business_name=scraped_data.business_name or "",
            url=task.url,
            business_info=scraped_data.business_info or "",
            existing_scripts=existing_scripts
        )
    except Exception as e:
        print("System variant script generation failed:", repr(e))
        return {
            "error": "System variant script generation failed",
            "details": str(e)
        }

    variant_record = AdVariant(task_id=task.id)
    db.add(variant_record)
    db.commit()
    db.refresh(variant_record)

    try:
        script_record = AdScript(
            variant_id=variant_record.id,
            script=script_text
        )
        db.add(script_record)
        db.commit()
        db.refresh(script_record)

    except Exception as e:
        print("System variant script save failed:", repr(e))
        db.rollback()
        return {
            "error": "System variant script save failed",
            "details": str(e),
            "task_id": task.id,
            "variant_id": variant_record.id,
            "url": task.url
        }

    try:
        generated_voices = generate_all_voices(
            script_text,
            task.id,
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
        print("System variant voice generation failed:", repr(e))
        db.rollback()
        return {
            "error": "System variant voice generation failed",
            "details": str(e),
            "task_id": task.id,
            "variant_id": variant_record.id,
            "url": task.url
        }

    voice_records = db.query(AdVoice).filter(
        AdVoice.variant_id == variant_record.id
    ).all()

    return {
        "task_id": task.id,
        "variant_id": variant_record.id,
        "url": task.url,
        "business_name": scraped_data.business_name or "",
        "logo": scraped_data.business_logo or "",
        "script": script_record.script,
        "images": scraped_data.images or [],
        "voices": serialize_voices(voice_records),
        "music": serialize_music(db)
    }


def create_custom_generated_variant(
    task_id: int,
    user_prompt: Optional[str],
    db: Session
):
    task, scraped_data = get_task_with_scraped_data(task_id, db)

    if not task:
        return {"error": "Task not found"}

    if not scraped_data:
        return {"error": "Scraped data not found for this task"}

    user_prompt = (user_prompt or "").strip()

    if not user_prompt:
        return {"error": "Custom prompt is required"}

    existing_scripts = get_existing_scripts_for_task(task_id, db)

    try:
        script_text = generate_custom_variant_script(
            business_name=scraped_data.business_name or "",
            url=task.url,
            business_info=scraped_data.business_info or "",
            existing_scripts=existing_scripts,
            user_prompt=user_prompt
        )
    except Exception as e:
        print("Custom variant script generation failed:", repr(e))
        return {
            "error": "Custom variant script generation failed",
            "details": str(e)
        }

    variant_record = AdVariant(task_id=task.id)
    db.add(variant_record)
    db.commit()
    db.refresh(variant_record)

    try:
        script_record = AdScript(
            variant_id=variant_record.id,
            script=script_text
        )
        db.add(script_record)
        db.commit()
        db.refresh(script_record)

    except Exception as e:
        print("Custom variant script save failed:", repr(e))
        db.rollback()
        return {
            "error": "Custom variant script save failed",
            "details": str(e),
            "task_id": task.id,
            "variant_id": variant_record.id,
            "url": task.url
        }

    try:
        generated_voices = generate_all_voices(
            script_text,
            task.id,
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
        print("Custom variant voice generation failed:", repr(e))
        db.rollback()
        return {
            "error": "Custom variant voice generation failed",
            "details": str(e),
            "task_id": task.id,
            "variant_id": variant_record.id,
            "url": task.url
        }

    voice_records = db.query(AdVoice).filter(
        AdVoice.variant_id == variant_record.id
    ).all()

    return {
        "task_id": task.id,
        "variant_id": variant_record.id,
        "url": task.url,
        "business_name": scraped_data.business_name or "",
        "logo": scraped_data.business_logo or "",
        "script": script_record.script,
        "images": scraped_data.images or [],
        "voices": serialize_voices(voice_records),
        "music": serialize_music(db)
    }