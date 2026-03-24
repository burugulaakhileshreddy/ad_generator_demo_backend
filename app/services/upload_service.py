import os
from fastapi import UploadFile

from app.services.storage_service import save_uploaded_user_image


def save_uploaded_image(file: UploadFile, task_id: int, variant_id: int):
    """
    Save uploaded user image using env-driven storage backend.

    Local mode:
    storage/user_uploads/task_<task_id>/variant_<variant_id>/

    Supabase mode:
    uploads to configured Supabase bucket/path

    Returns:
        stored file reference (local relative path or absolute Supabase URL)
    """

    file_bytes = file.file.read()
    original_filename = file.filename or "upload.jpg"

    return save_uploaded_user_image(
        file_bytes=file_bytes,
        original_filename=original_filename,
        task_id=task_id,
        variant_id=variant_id
    )