import os
import uuid
import hashlib
import mimetypes
import shutil
from io import BytesIO
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from PIL import Image
from dotenv import load_dotenv

load_dotenv()

try:
    from supabase import create_client, Client
except Exception:
    Client = None
    create_client = None


# ---------------------------------------------------------
# ENV / CONFIG
# ---------------------------------------------------------

STORAGE_PROVIDER = os.getenv("STORAGE_PROVIDER", "local").strip().lower()

LOCAL_STORAGE_BASE_URL = os.getenv(
    "LOCAL_STORAGE_BASE_URL",
    "http://127.0.0.1:8000"
).rstrip("/")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

SUPABASE_BUCKET_IMAGES = os.getenv("SUPABASE_BUCKET_IMAGES", "images").strip()
SUPABASE_BUCKET_AUDIO = os.getenv("SUPABASE_BUCKET_AUDIO", "audio").strip()
SUPABASE_BUCKET_VIDEOS = os.getenv("SUPABASE_BUCKET_VIDEOS", "videos").strip()
SUPABASE_BUCKET_USER_UPLOADS = os.getenv("SUPABASE_BUCKET_USER_UPLOADS", "user_uploads").strip()

BASE_STORAGE_PATH = "storage/images"
MAX_IMAGES = 20

AUDIO_BASE_PATH = "storage/audio"
VIDEOS_BASE_PATH = "storage/videos"
USER_UPLOADS_BASE_PATH = "storage/user_uploads"
TEMP_BASE_PATH = "storage/temp"
TEMP_REMOTE_ASSETS_PATH = os.path.join(TEMP_BASE_PATH, "remote_assets")
TEMP_RENDER_OUTPUTS_PATH = os.path.join(TEMP_BASE_PATH, "render_outputs")

SUPABASE_CLIENT: Optional["Client"] = None

if STORAGE_PROVIDER == "supabase":
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise ValueError("Supabase storage selected but SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY missing")

    if create_client is None:
        raise ImportError("supabase package is required when STORAGE_PROVIDER=supabase")

    SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# ---------------------------------------------------------
# GENERIC HELPERS
# ---------------------------------------------------------

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _is_absolute_url(value: Optional[str]) -> bool:
    if not value:
        return False
    value = str(value).strip().lower()
    return value.startswith("http://") or value.startswith("https://")


def _guess_content_type(path_or_name: str, default: str = "application/octet-stream") -> str:
    content_type, _ = mimetypes.guess_type(path_or_name)
    return content_type or default


def _upload_bytes_to_supabase(bucket: str, object_path: str, data: bytes, content_type: str) -> str:
    if not SUPABASE_CLIENT:
        raise Exception("Supabase client is not initialized")

    SUPABASE_CLIENT.storage.from_(bucket).upload(
        path=object_path,
        file=data,
        file_options={
            "content-type": content_type,
            "upsert": "true"
        }
    )

    return SUPABASE_CLIENT.storage.from_(bucket).get_public_url(object_path)


def _local_public_path(local_path: str) -> str:
    return local_path.replace("\\", "/")


def _write_local_file(path: str, data: bytes) -> str:
    path = path.replace("\\", "/")
    _ensure_dir(os.path.dirname(path))
    with open(path, "wb") as f:
        f.write(data)
    return path


# ---------------------------------------------------------
# IMAGE DIRECTORY HELPERS
# ---------------------------------------------------------

def create_task_directory(task_id: int):
    task_path = os.path.join(BASE_STORAGE_PATH, f"task_{task_id}")
    os.makedirs(task_path, exist_ok=True)
    return task_path


def clear_task_image_directory(task_id: int):
    """
    Clears local image directory for a task completely,
    then recreates it empty.
    """
    task_path = os.path.join(BASE_STORAGE_PATH, f"task_{task_id}")

    if os.path.exists(task_path):
        shutil.rmtree(task_path, ignore_errors=True)

    os.makedirs(task_path, exist_ok=True)
    return task_path


def _list_supabase_prefix_files(bucket: str, prefix: str):
    """
    Lists files under a prefix recursively in Supabase storage.
    """
    if not SUPABASE_CLIENT:
        return []

    all_paths = []

    def walk(path_prefix: str):
        try:
            items = SUPABASE_CLIENT.storage.from_(bucket).list(path_prefix)
        except Exception as e:
            print(f"[STORAGE] Failed listing Supabase prefix '{path_prefix}': {e}")
            return

        if not items:
            return

        for item in items:
            name = item.get("name")
            if not name:
                continue

            nested_id = item.get("id")
            item_path = f"{path_prefix}/{name}" if path_prefix else name

            # Supabase list may return folders and files;
            # folders usually do not have metadata like id/updated_at in the same way.
            if nested_id:
                all_paths.append(item_path)
            else:
                walk(item_path)

    walk(prefix)
    return all_paths


def clear_task_images_supabase(task_id: int):
    """
    Clears all image objects for a task under:
    images/task_<task_id>/...
    """
    if not SUPABASE_CLIENT:
        return

    prefix = f"task_{task_id}"
    paths_to_remove = _list_supabase_prefix_files(SUPABASE_BUCKET_IMAGES, prefix)

    if not paths_to_remove:
        return

    try:
        SUPABASE_CLIENT.storage.from_(SUPABASE_BUCKET_IMAGES).remove(paths_to_remove)
        print(f"[STORAGE] Cleared Supabase image prefix: {prefix} ({len(paths_to_remove)} files)")
    except Exception as e:
        print(f"[STORAGE] Failed clearing Supabase image prefix '{prefix}': {e}")


def clear_task_image_storage(task_id: int):
    """
    Clears task image storage for both local and Supabase modes.
    Call this ONCE at the start of a fresh scrape before logo/images are saved.
    """
    if STORAGE_PROVIDER == "supabase":
        clear_task_images_supabase(task_id)
    else:
        clear_task_image_directory(task_id)


def create_audio_directory(task_id: int, variant_id: int):
    variant_path = os.path.join(
        AUDIO_BASE_PATH,
        f"task_{task_id}",
        f"variant_{variant_id}"
    )
    os.makedirs(variant_path, exist_ok=True)
    return variant_path


def create_user_upload_directory(task_id: int, variant_id: int):
    variant_path = os.path.join(
        USER_UPLOADS_BASE_PATH,
        f"task_{task_id}",
        f"variant_{variant_id}"
    )
    os.makedirs(variant_path, exist_ok=True)
    return variant_path


def create_temp_output_directory():
    os.makedirs(TEMP_RENDER_OUTPUTS_PATH, exist_ok=True)
    return TEMP_RENDER_OUTPUTS_PATH


# ---------------------------------------------------------
# IMAGE CONVERSION
# ---------------------------------------------------------

def convert_to_jpg(image_bytes):
    try:
        image = Image.open(BytesIO(image_bytes)).convert("RGB")
        output = BytesIO()
        image.save(output, format="JPEG", quality=90)
        return output.getvalue()
    except Exception:
        return None


# ---------------------------------------------------------
# IMAGE VALIDATION (SMART)
# ---------------------------------------------------------

def is_valid_image(width, height):
    area = width * height

    if area < 30000:
        return False

    aspect = width / height

    if aspect > 6 or aspect < 0.15:
        return False

    return True


# ---------------------------------------------------------
# IMAGE SCORING (VIBE++)
# ---------------------------------------------------------

def score_image(width, height):
    score = width * height
    aspect = width / height

    if 1.3 < aspect < 2.5:
        score *= 1.8
    elif 0.8 < aspect < 1.2:
        score *= 1.2

    return score


# ---------------------------------------------------------
# IMAGE CATEGORY
# ---------------------------------------------------------

def categorize_image(width, height):
    aspect = width / height

    if aspect > 1.5:
        return "hero"
    elif 0.8 < aspect < 1.3:
        return "product"
    else:
        return "background"


# ---------------------------------------------------------
# FILTER JUNK URLS
# ---------------------------------------------------------

def is_bad_url(url: str):
    url = url.lower()

    bad_keywords = [
        "logo", "icon", "sprite", "badge",
        "avatar", "thumbnail", "thumb",
        "poster", "placeholder", "favicon"
    ]

    return any(k in url for k in bad_keywords)


# ---------------------------------------------------------
# OPTIONAL BLANK IMAGE CHECK
# ---------------------------------------------------------

def is_blank_image(image: Image.Image):
    try:
        small = image.resize((50, 50))
        colors = small.getcolors(2500)

        if not colors:
            return True

        if len(colors) < 10:
            return True

        return False
    except Exception:
        return False


# ---------------------------------------------------------
# PROCESS SINGLE IMAGE URL
# ---------------------------------------------------------

def process_image_url(url, session, headers):
    if is_bad_url(url):
        return {
            "status": "skipped_bad_url",
            "candidate": None
        }

    try:
        response = session.get(url, headers=headers, timeout=6)

        if response.status_code != 200:
            return {
                "status": "http_fail",
                "candidate": None
            }

        jpg_bytes = convert_to_jpg(response.content)

        if not jpg_bytes:
            return {
                "status": "invalid_image",
                "candidate": None
            }

        image = Image.open(BytesIO(jpg_bytes))
        width, height = image.size

        if not is_valid_image(width, height):
            return {
                "status": "filtered_small",
                "candidate": None
            }

        img_hash = hashlib.md5(jpg_bytes[:5000]).hexdigest()
        score = score_image(width, height)
        category = categorize_image(width, height)

        return {
            "status": "ok",
            "candidate": {
                "score": score,
                "bytes": jpg_bytes,
                "category": category,
                "hash": img_hash
            }
        }

    except Exception:
        return {
            "status": "exception",
            "candidate": None
        }


# ---------------------------------------------------------
# IMAGE SAVE HELPERS
# ---------------------------------------------------------

def _store_image_bytes(task_id: int, filename: str, data: bytes) -> str:
    if STORAGE_PROVIDER == "supabase":
        object_path = f"task_{task_id}/{filename}"
        return _upload_bytes_to_supabase(
            bucket=SUPABASE_BUCKET_IMAGES,
            object_path=object_path,
            data=data,
            content_type="image/jpeg"
        )

    task_path = create_task_directory(task_id)
    file_path = os.path.join(task_path, filename)
    return _write_local_file(file_path, data)


def _store_svg_logo(task_id: int, data: bytes) -> str:
    if STORAGE_PROVIDER == "supabase":
        object_path = f"task_{task_id}/logo.svg"
        return _upload_bytes_to_supabase(
            bucket=SUPABASE_BUCKET_IMAGES,
            object_path=object_path,
            data=data,
            content_type="image/svg+xml"
        )

    task_path = create_task_directory(task_id)
    file_path = os.path.join(task_path, "logo.svg")
    return _write_local_file(file_path, data)


# ---------------------------------------------------------
# DOWNLOAD IMAGES
# ---------------------------------------------------------

def download_images(image_urls, task_id):
    print(f"[STORAGE] Incoming URLs: {len(image_urls)}")

    saved_images = []
    image_candidates = []
    seen_hashes = set()

    headers = {"User-Agent": "Mozilla/5.0"}

    download_success = 0
    filtered_invalid = 0
    filtered_small = 0
    duplicates = 0

    max_workers = 8 if len(image_urls) > 40 else 4

    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_image_url, url, session, headers): url
                for url in image_urls
            }

            for future in as_completed(futures):
                result = future.result()

                status = result["status"]
                candidate = result["candidate"]

                if status == "ok" and candidate:
                    download_success += 1

                    img_hash = candidate["hash"]

                    if img_hash in seen_hashes:
                        duplicates += 1
                        continue

                    seen_hashes.add(img_hash)

                    image_candidates.append({
                        "score": candidate["score"],
                        "bytes": candidate["bytes"],
                        "category": candidate["category"]
                    })

                elif status == "invalid_image":
                    filtered_invalid += 1

                elif status == "filtered_small":
                    filtered_small += 1

    print(f"[STORAGE] Download success: {download_success}")
    print(f"[STORAGE] Filtered (invalid): {filtered_invalid}")
    print(f"[STORAGE] Filtered (small/aspect): {filtered_small}")
    print(f"[STORAGE] Duplicates removed: {duplicates}")
    print(f"[STORAGE] Valid candidates: {len(image_candidates)}")

    image_candidates.sort(key=lambda x: x["score"], reverse=True)

    final_selection = []

    hero = [i for i in image_candidates if i["category"] == "hero"]
    product = [i for i in image_candidates if i["category"] == "product"]
    background = [i for i in image_candidates if i["category"] == "background"]

    final_selection.extend(hero[:8])
    final_selection.extend(product[:6])
    final_selection.extend(background[:6])

    if len(final_selection) < MAX_IMAGES:
        final_selection = image_candidates[:MAX_IMAGES]

    for i, img in enumerate(final_selection[:MAX_IMAGES]):
        stored_ref = _store_image_bytes(
            task_id=task_id,
            filename=f"image_{i}.jpg",
            data=img["bytes"]
        )
        saved_images.append(stored_ref)

    print(f"[STORAGE] Final stored: {len(saved_images)}")

    return saved_images


# ---------------------------------------------------------
# DOWNLOAD LOGO
# ---------------------------------------------------------

def download_logo(logo_url, task_id):
    if not logo_url:
        return None

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(logo_url, headers=headers, timeout=6)

        if r.status_code != 200:
            return None

        if logo_url.lower().endswith(".svg"):
            return _store_svg_logo(task_id, r.content)

        jpg_bytes = convert_to_jpg(r.content)

        if not jpg_bytes:
            return None

        return _store_image_bytes(
            task_id=task_id,
            filename="logo.jpg",
            data=jpg_bytes
        )

    except Exception:
        return None


# ---------------------------------------------------------
# AUDIO STORAGE
# ---------------------------------------------------------

def store_voice_audio(task_id: int, variant_id: int, voice_name: str, audio_bytes: bytes) -> str:
    if STORAGE_PROVIDER == "supabase":
        object_path = f"task_{task_id}/variant_{variant_id}/{voice_name}.mp3"
        return _upload_bytes_to_supabase(
            bucket=SUPABASE_BUCKET_AUDIO,
            object_path=object_path,
            data=audio_bytes,
            content_type="audio/mpeg"
        )

    audio_dir = create_audio_directory(task_id, variant_id)
    file_path = os.path.join(audio_dir, f"{voice_name}.mp3")
    return _write_local_file(file_path, audio_bytes)


# ---------------------------------------------------------
# USER UPLOAD STORAGE
# ---------------------------------------------------------

def save_uploaded_user_image(file_bytes: bytes, original_filename: str, task_id: int, variant_id: int) -> str:
    extension = os.path.splitext(original_filename or "")[1].lower().strip()
    if extension not in [".jpg", ".jpeg", ".png", ".webp"]:
        extension = ".jpg"

    filename = f"{uuid.uuid4().hex[:12]}{extension}"
    content_type = _guess_content_type(filename, "image/jpeg")

    if STORAGE_PROVIDER == "supabase":
        object_path = f"task_{task_id}/variant_{variant_id}/{filename}"
        return _upload_bytes_to_supabase(
            bucket=SUPABASE_BUCKET_USER_UPLOADS,
            object_path=object_path,
            data=file_bytes,
            content_type=content_type
        )

    upload_dir = create_user_upload_directory(task_id, variant_id)
    file_path = os.path.join(upload_dir, filename)
    return _write_local_file(file_path, file_bytes)


# ---------------------------------------------------------
# VIDEO OUTPUT PATHS
# ---------------------------------------------------------

def get_video_output_path(task_id: int):
    """
    Returns local temp output path where ffmpeg writes final MP4 first.
    This remains local in both modes because ffmpeg needs a filesystem path.
    """
    videos_dir = create_temp_output_directory()

    unique_id = uuid.uuid4().hex[:10]

    return os.path.join(
        videos_dir,
        f"task_{task_id}_{unique_id}.mp4"
    )


def store_rendered_video(local_temp_video_path: str, task_id: int, variant_id: int) -> str:
    if not os.path.exists(local_temp_video_path):
        raise FileNotFoundError(f"Rendered temp video not found: {local_temp_video_path}")

    filename = f"task_{task_id}_variant_{variant_id}_{uuid.uuid4().hex[:10]}.mp4"

    with open(local_temp_video_path, "rb") as f:
        video_bytes = f.read()

    if STORAGE_PROVIDER == "supabase":
        object_path = f"task_{task_id}/variant_{variant_id}/{filename}"
        stored_ref = _upload_bytes_to_supabase(
            bucket=SUPABASE_BUCKET_VIDEOS,
            object_path=object_path,
            data=video_bytes,
            content_type="video/mp4"
        )
        return stored_ref

    _ensure_dir(VIDEOS_BASE_PATH)
    final_path = os.path.join(VIDEOS_BASE_PATH, filename)
    return _write_local_file(final_path, video_bytes)


# ---------------------------------------------------------
# REMOTE ASSET MATERIALIZATION FOR RENDERING
# ---------------------------------------------------------

def materialize_asset_to_local(asset_ref: Optional[str]) -> Optional[str]:
    """
    Converts an asset reference into a local filesystem path usable by PIL/ffmpeg.

    Supported:
    - local filesystem path
    - /storage/... style path
    - absolute local public URL
    - absolute remote URL (downloads to temp)
    """
    if not asset_ref:
        return None

    asset_ref = str(asset_ref).strip()

    if asset_ref.startswith(LOCAL_STORAGE_BASE_URL + "/"):
        asset_ref = asset_ref.replace(LOCAL_STORAGE_BASE_URL + "/", "", 1)

    if _is_absolute_url(asset_ref):
        _ensure_dir(TEMP_REMOTE_ASSETS_PATH)

        parsed_name = asset_ref.split("?")[0].split("/")[-1] or f"{uuid.uuid4().hex}.bin"
        ext = os.path.splitext(parsed_name)[1] or ".bin"
        temp_name = f"{uuid.uuid4().hex[:16]}{ext}"
        temp_path = os.path.join(TEMP_REMOTE_ASSETS_PATH, temp_name)

        response = requests.get(asset_ref, timeout=20)
        response.raise_for_status()

        with open(temp_path, "wb") as f:
            f.write(response.content)

        return temp_path.replace("\\", "/")

    return asset_ref.lstrip("/").replace("\\", "/")
