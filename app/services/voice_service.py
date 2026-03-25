
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from openai import OpenAI

from app.services.storage_service import store_voice_audio

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# ---------------------------------------------------------
# AVAILABLE VOICES
# ---------------------------------------------------------

VOICES = [
    ("v1", "alloy"),
    ("v2", "nova")
]


# ---------------------------------------------------------
# GENERATE SINGLE VOICE BYTES
# ---------------------------------------------------------

def generate_voice_bytes(script_text: str, voice: str) -> bytes:
    response = client.audio.speech.create(
        model="gpt-4o-mini-tts",
        voice=voice,
        input=script_text
    )

    return response.read()


# ---------------------------------------------------------
# PARALLEL VOICE WORKER
# ---------------------------------------------------------

def _generate_single_voice_result(script_text: str, voice_name: str, voice: str, task_id: int, variant_id: int):
    audio_bytes = generate_voice_bytes(script_text, voice)

    stored_audio_path = store_voice_audio(
        task_id=task_id,
        variant_id=variant_id,
        voice_name=voice_name,
        audio_bytes=audio_bytes
    )

    return {
        "voice_name": voice_name,
        "audio_path": stored_audio_path
    }


# ---------------------------------------------------------
# GENERATE ALL VOICES
# ---------------------------------------------------------

def generate_all_voices(script_text: str, task_id: int, variant_id: int):
    results_by_name = {}

    max_workers = min(len(VOICES), 2)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        for voice_name, voice in VOICES:
            future = executor.submit(
                _generate_single_voice_result,
                script_text,
                voice_name,
                voice,
                task_id,
                variant_id
            )
            futures[future] = voice_name

        for future in as_completed(futures):
            voice_name = futures[future]

            try:
                result = future.result()
                results_by_name[voice_name] = result

            except Exception as e:
                print("Voice generation failed:", voice_name, e)

    audio_results = []

    for voice_name, _ in VOICES:
        if voice_name in results_by_name:
            audio_results.append(results_by_name[voice_name])

    return audio_results