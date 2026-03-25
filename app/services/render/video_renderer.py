# RY96 - Production Video Renderer

import os
import subprocess
from typing import Optional, Tuple, List

import qrcode
from PIL import Image, ImageDraw, ImageFont

from sqlalchemy.orm import Session

from app.models.ad_video_model import AdVideo
from app.services.storage_service import (
    get_video_output_path,
    store_rendered_video,
    materialize_asset_to_local
)


# ---------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------

# Final output resolution
FRAME_W = 854
FRAME_H = 480

PREVIEW_W = 900
PREVIEW_H = 420

END_SCREEN_DURATION = 3.0
TRANSITION_DURATION = 0.6

TEMP_RENDER_DIR = "storage/temp/render_frames"
ICON_DIR = "storage/static/icons"

LOCATION_ICON = os.path.join(ICON_DIR, "location.png").replace("\\", "/")
PHONE_ICON = os.path.join(ICON_DIR, "phone.png").replace("\\", "/")
GLOBE_ICON = os.path.join(ICON_DIR, "globe.png").replace("\\", "/")
SOCIAL_ICON = os.path.join(ICON_DIR, "social.png").replace("\\", "/")

# Base design size used by original working renderer
BASE_FRAME_W = 1920
BASE_FRAME_H = 1080

SCALE_X = FRAME_W / BASE_FRAME_W
SCALE_Y = FRAME_H / BASE_FRAME_H
SCALE = min(SCALE_X, SCALE_Y)

print("LOCATION_ICON:", LOCATION_ICON, os.path.exists(LOCATION_ICON))
print("PHONE_ICON:", PHONE_ICON, os.path.exists(PHONE_ICON))
print("GLOBE_ICON:", GLOBE_ICON, os.path.exists(GLOBE_ICON))
print("SOCIAL_ICON:", SOCIAL_ICON, os.path.exists(SOCIAL_ICON))


# ---------------------------------------------------------
# SCALE HELPERS
# ---------------------------------------------------------

def _sx(value: int) -> int:
    return max(1, int(round(value * SCALE_X)))


def _sy(value: int) -> int:
    return max(1, int(round(value * SCALE_Y)))


def _ss(value: int) -> int:
    return max(1, int(round(value * SCALE)))


# ---------------------------------------------------------
# PATH / IO HELPERS
# ---------------------------------------------------------

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _normalize_local_path(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return materialize_asset_to_local(value)


def _safe_open_rgba(path: Optional[str]) -> Optional[Image.Image]:
    if not path:
        return None
    if not os.path.exists(path):
        return None
    try:
        return Image.open(path).convert("RGBA")
    except Exception:
        return None


def _run_subprocess(cmd: List[str]):
    subprocess.run(cmd, check=True)


def _get_audio_duration(path: Optional[str]) -> Optional[float]:
    if not path or not os.path.exists(path):
        return None

    try:
        output = subprocess.check_output([
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            path
        ]).decode().strip()

        return float(output)
    except Exception:
        return None


# ---------------------------------------------------------
# FONT HELPERS
# ---------------------------------------------------------

def _pick_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    scaled_size = _ss(size)
    candidates = []

    if bold:
        candidates.extend([
            "C:/Windows/Fonts/arialbd.ttf",
            "C:/Windows/Fonts/segoeuib.ttf",
            "C:/Windows/Fonts/calibrib.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ])
    else:
        candidates.extend([
            "C:/Windows/Fonts/arial.ttf",
            "C:/Windows/Fonts/segoeui.ttf",
            "C:/Windows/Fonts/calibri.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ])

    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size=scaled_size)
            except Exception:
                pass

    return ImageFont.load_default()


# ---------------------------------------------------------
# IMAGE HELPERS
# ---------------------------------------------------------

def _cover_resize(img: Image.Image, target_w: int, target_h: int) -> Image.Image:
    src_w, src_h = img.size
    src_ratio = src_w / src_h
    target_ratio = target_w / target_h

    if src_ratio > target_ratio:
        new_h = target_h
        new_w = int(new_h * src_ratio)
    else:
        new_w = target_w
        new_h = int(new_w / src_ratio)

    resized = img.resize((new_w, new_h), Image.LANCZOS)

    left = (new_w - target_w) // 2
    top = (new_h - target_h) // 2

    return resized.crop((left, top, left + target_w, top + target_h))


def _contain_resize(img: Image.Image, target_w: int, target_h: int, padding: int = 0) -> Image.Image:
    box_w = max(1, target_w - padding * 2)
    box_h = max(1, target_h - padding * 2)

    copy_img = img.copy()
    copy_img.thumbnail((box_w, box_h), Image.LANCZOS)

    canvas = Image.new("RGBA", (target_w, target_h), (255, 255, 255, 0))
    x = (target_w - copy_img.width) // 2
    y = (target_h - copy_img.height) // 2
    canvas.paste(copy_img, (x, y), copy_img)
    return canvas


def _rounded_rect_mask(size: Tuple[int, int], radius: int) -> Image.Image:
    mask = Image.new("L", size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle((0, 0, size[0], size[1]), radius=radius, fill=255)
    return mask


def _paste_with_shadow(
    base: Image.Image,
    overlay: Image.Image,
    x: int,
    y: int,
    shadow_offset: Tuple[int, int] = (0, 8),
    shadow_alpha: int = 55,
    shadow_expand: int = 16,
    shadow_radius: int = 12
):
    shadow = Image.new("RGBA", (overlay.width + shadow_expand * 2, overlay.height + shadow_expand * 2), (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)

    sx1 = shadow_expand
    sy1 = shadow_expand
    sx2 = shadow_expand + overlay.width
    sy2 = shadow_expand + overlay.height

    shadow_draw.rounded_rectangle(
        (sx1, sy1, sx2, sy2),
        radius=max(8, shadow_radius),
        fill=(0, 0, 0, shadow_alpha)
    )

    shadow = shadow.resize((max(1, shadow.width // 2), max(1, shadow.height // 2)), Image.LANCZOS)
    shadow = shadow.resize((overlay.width + shadow_expand * 2, overlay.height + shadow_expand * 2), Image.LANCZOS)

    base.alpha_composite(
        shadow,
        (x - shadow_expand + shadow_offset[0], y - shadow_expand + shadow_offset[1])
    )
    base.alpha_composite(overlay, (x, y))


def _paste_icon(base: Image.Image, icon_path: str, x: int, y: int, size: int = 24, opacity: int = 255):
    print("Trying icon:", icon_path)

    if not os.path.exists(icon_path):
        print("Icon not found:", icon_path)
        return

    try:
        icon = Image.open(icon_path).convert("RGBA")
        icon = icon.resize((size, size), Image.LANCZOS)

        if opacity < 255:
            alpha = icon.getchannel("A")
            alpha = alpha.point(lambda p: int(p * (opacity / 255)))
            icon.putalpha(alpha)

        base.alpha_composite(icon, (x, y))
        print("Icon pasted:", icon_path)

    except Exception as e:
        print("Icon paste failed:", icon_path, e)


# ---------------------------------------------------------
# TEXT HELPERS
# ---------------------------------------------------------

def _text_size(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont) -> Tuple[int, int]:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def _draw_text_with_shadow(
    draw: ImageDraw.ImageDraw,
    xy: Tuple[int, int],
    text: str,
    font: ImageFont.FreeTypeFont,
    fill: Tuple[int, int, int, int],
    shadow_fill: Tuple[int, int, int, int] = (0, 0, 0, 120),
    shadow_offsets: List[Tuple[int, int]] = None
):
    if shadow_offsets is None:
        shadow_offsets = [(_ss(1), _ss(1)), (0, _ss(2)), (0, _ss(4))]

    x, y = xy
    for dx, dy in shadow_offsets:
        draw.text((x + dx, y + dy), text, font=font, fill=shadow_fill)
    draw.text((x, y), text, font=font, fill=fill)


def _truncate_to_width(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.FreeTypeFont,
    max_width: int
) -> str:
    if not text:
        return ""

    current = text
    while current:
        w, _ = _text_size(draw, current, font)
        if w <= max_width:
            return current
        current = current[:-1]

    return ""


def _wrap_text_to_width(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.FreeTypeFont,
    max_width: int
) -> str:
    if not text:
        return ""

    words = text.split()
    if not words:
        return ""

    lines = []
    current_line = words[0]

    for word in words[1:]:
        candidate = f"{current_line} {word}"
        w, _ = _text_size(draw, candidate, font)
        if w <= max_width:
            current_line = candidate
        else:
            lines.append(current_line)
            current_line = word

    lines.append(current_line)
    return "\n".join(lines)


# ---------------------------------------------------------
# FRAME BUILDERS
# ---------------------------------------------------------

def _build_qr_card(qr_value: str, size: int) -> Image.Image:
    qr_img = qrcode.make(qr_value).convert("RGBA")
    qr_img = qr_img.resize((size, size), Image.NEAREST)

    pad = int(size * 0.15)
    card_w = size + pad * 2
    card_h = size + pad * 2

    mask = _rounded_rect_mask((card_w, card_h), radius=max(8, int(size * 0.08)))
    card = Image.new("RGBA", (card_w, card_h), (255, 255, 255, 255))
    card.putalpha(mask)

    card.alpha_composite(qr_img, (pad, pad))
    return card


def _build_slide_frame(
    slide_path: str,
    banner_data: dict,
    banner_enabled: bool,
    qr_enabled: bool,
    qr_url: Optional[str],
    frame_index: int
) -> str:
    _ensure_dir(TEMP_RENDER_DIR)

    canvas = Image.new("RGBA", (FRAME_W, FRAME_H), (0, 0, 0, 255))

    slide_img = _safe_open_rgba(slide_path)
    if slide_img is None:
        raise Exception(f"Slide not found or unreadable: {slide_path}")

    bg = _cover_resize(slide_img, FRAME_W, FRAME_H)
    canvas.alpha_composite(bg, (0, 0))

    if qr_enabled and qr_url:
        qr_size = _ss(172)
        qr_card = _build_qr_card(qr_url, qr_size)

        qr_x = FRAME_W - qr_card.width - _sx(34)
        qr_y = _sy(34)

        _paste_with_shadow(
            canvas,
            qr_card,
            qr_x,
            qr_y,
            shadow_offset=(0, _sy(10)),
            shadow_alpha=60,
            shadow_expand=_ss(18),
            shadow_radius=_ss(16)
        )

    if banner_enabled and banner_data:
        draw = ImageDraw.Draw(canvas)

        left_x = _sx(34)
        bottom_y = FRAME_H - _sy(42)

        logo_src = _normalize_local_path(banner_data.get("logo"))
        logo_img = _safe_open_rgba(logo_src)

        company_name = (banner_data.get("companyName") or "").strip()
        address = (banner_data.get("address") or "").strip()
        phone = (banner_data.get("phone") or "").strip()
        website = (banner_data.get("website") or "").strip()

        logo_box_w = _sx(188)
        logo_box_h = _sy(188)

        info_x = left_x + (logo_box_w + _sx(26) if logo_img else 0)
        info_y = bottom_y - _sy(152)

        title_font = _pick_font(46, bold=True)
        meta_font = _pick_font(28, bold=False)

        if logo_img:
            logo_card = Image.new("RGBA", (logo_box_w, logo_box_h), (255, 255, 255, 255))
            logo_mask = _rounded_rect_mask((logo_box_w, logo_box_h), radius=_ss(10))
            logo_card.putalpha(logo_mask)

            contained_logo = _contain_resize(logo_img, logo_box_w, logo_box_h, padding=_ss(14))
            logo_card.alpha_composite(contained_logo, (0, 0))

            _paste_with_shadow(
                canvas,
                logo_card,
                left_x,
                bottom_y - logo_box_h,
                shadow_offset=(0, _sy(12)),
                shadow_alpha=58,
                shadow_expand=_ss(20),
                shadow_radius=_ss(14)
            )

        meta_max_w = int(FRAME_W * 0.45)

        if company_name:
            title = _truncate_to_width(draw, company_name, title_font, meta_max_w)
            _draw_text_with_shadow(
                draw,
                (info_x, info_y),
                title,
                title_font,
                fill=(255, 255, 255, 255),
                shadow_fill=(0, 0, 0, 140),
                shadow_offsets=[(0, _ss(2)), (0, _ss(5)), (_ss(1), _ss(1))]
            )

        meta_y = info_y + _sy(62)
        line_gap = _sy(12)
        icon_size = _ss(24)
        text_gap = _sx(10)
        row_height = _sy(34) + line_gap
        icon_opacity = 255

        if address:
            text_x = info_x + icon_size + text_gap
            text_y = meta_y
            icon_y = meta_y + _sy(2)

            _paste_icon(canvas, LOCATION_ICON, info_x, icon_y, size=icon_size, opacity=icon_opacity)

            line = _truncate_to_width(draw, address, meta_font, meta_max_w - icon_size - text_gap)
            _draw_text_with_shadow(
                draw,
                (text_x, text_y),
                line,
                meta_font,
                fill=(235, 235, 235, 245),
                shadow_fill=(0, 0, 0, 120)
            )
            meta_y += row_height

        if phone:
            text_x = info_x + icon_size + text_gap
            text_y = meta_y
            icon_y = meta_y + _sy(2)

            _paste_icon(canvas, PHONE_ICON, info_x, icon_y, size=icon_size, opacity=icon_opacity)

            line = _truncate_to_width(draw, phone, meta_font, meta_max_w - icon_size - text_gap)
            _draw_text_with_shadow(
                draw,
                (text_x, text_y),
                line,
                meta_font,
                fill=(235, 235, 235, 235),
                shadow_fill=(0, 0, 0, 120)
            )
            meta_y += row_height

        if website:
            text_x = info_x + icon_size + text_gap
            text_y = meta_y
            icon_y = meta_y + _sy(2)

            _paste_icon(canvas, GLOBE_ICON, info_x, icon_y, size=icon_size, opacity=icon_opacity)

            line = _truncate_to_width(draw, website, meta_font, meta_max_w - icon_size - text_gap)
            _draw_text_with_shadow(
                draw,
                (text_x, text_y),
                line,
                meta_font,
                fill=(225, 225, 225, 225),
                shadow_fill=(0, 0, 0, 120)
            )

    output_path = os.path.join(TEMP_RENDER_DIR, f"task_slide_{frame_index}.png").replace("\\", "/")
    canvas.convert("RGB").save(output_path, quality=95)
    return output_path


def _build_end_screen_frame(end_screen: dict, frame_index: int) -> Optional[str]:
    if not end_screen or not end_screen.get("enabled"):
        return None

    _ensure_dir(TEMP_RENDER_DIR)

    canvas = Image.new("RGBA", (FRAME_W, FRAME_H), (255, 255, 255, 255))
    draw = ImageDraw.Draw(canvas)

    offer = (end_screen.get("data", {}).get("offer") or "").strip()
    company_name = (end_screen.get("data", {}).get("companyName") or "").strip()
    address = (end_screen.get("data", {}).get("address") or "").strip()
    phone = (end_screen.get("data", {}).get("phone") or "").strip()
    website = (end_screen.get("data", {}).get("website") or "").strip()
    social_links = end_screen.get("data", {}).get("socialLinks") or []
    logo_src = _normalize_local_path(end_screen.get("data", {}).get("logo"))
    logo_img = _safe_open_rgba(logo_src)

    offer_font = _pick_font(74, bold=True)
    company_font = _pick_font(52, bold=True)
    body_font = _pick_font(34, bold=False)
    social_font = _pick_font(28, bold=False)

    center_x = FRAME_W // 2
    y = _sy(140)

    if offer:
        offer_w, offer_h = _text_size(draw, offer, offer_font)
        draw.text(
            (center_x - offer_w // 2, y),
            offer,
            font=offer_font,
            fill=(147, 51, 234, 255)
        )
        y += offer_h + _sy(42)

    if logo_img:
        logo_box = _contain_resize(logo_img, _sx(260), _sy(150), padding=0)
        lx = center_x - logo_box.width // 2
        canvas.alpha_composite(logo_box, (lx, y))
        y += _sy(150) + _sy(34)

    if company_name:
        company_w, company_h = _text_size(draw, company_name, company_font)
        draw.text(
            (center_x - company_w // 2, y),
            company_name,
            font=company_font,
            fill=(17, 24, 39, 255)
        )
        y += company_h + _sy(18)

    info_block_max_w = _sx(1200)
    icon_size = _ss(28)
    text_gap = _sx(14)
    icon_to_text_total = icon_size + text_gap

    if address:
        wrapped = _wrap_text_to_width(draw, address, body_font, info_block_max_w - icon_to_text_total)
        bbox = draw.multiline_textbbox((0, 0), wrapped, font=body_font, spacing=_sy(8), align="left")
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]

        block_w = icon_to_text_total + text_w
        start_x = center_x - (block_w // 2)
        icon_y = y + _sy(4)
        text_x = start_x + icon_to_text_total

        _paste_icon(canvas, LOCATION_ICON, start_x, icon_y, size=icon_size, opacity=255)

        draw.multiline_text(
            (text_x, y),
            wrapped,
            font=body_font,
            fill=(107, 114, 128, 255),
            spacing=_sy(8),
            align="left"
        )
        y += text_h + _sy(16)

    if phone:
        phone_w, phone_h = _text_size(draw, phone, body_font)
        block_w = icon_to_text_total + phone_w
        start_x = center_x - (block_w // 2)
        icon_y = y + _sy(4)
        text_x = start_x + icon_to_text_total

        _paste_icon(canvas, PHONE_ICON, start_x, icon_y, size=icon_size, opacity=255)

        draw.text(
            (text_x, y),
            phone,
            font=body_font,
            fill=(79, 70, 229, 255)
        )
        y += phone_h + _sy(12)

    if website:
        web_w, web_h = _text_size(draw, website, body_font)
        block_w = icon_to_text_total + web_w
        start_x = center_x - (block_w // 2)
        icon_y = y + _sy(4)
        text_x = start_x + icon_to_text_total

        _paste_icon(canvas, GLOBE_ICON, start_x, icon_y, size=icon_size, opacity=255)

        draw.text(
            (text_x, y),
            website,
            font=body_font,
            fill=(156, 163, 175, 255)
        )

    if social_links:
        valid_social_links = [str(s).strip() for s in social_links if str(s).strip()]
        if valid_social_links:
            social_text = "  |  ".join(valid_social_links)
            social_w, social_h = _text_size(draw, social_text, social_font)
            block_w = icon_to_text_total + social_w
            start_x = center_x - (block_w // 2)
            icon_y = FRAME_H - _sy(120) + _sy(2)
            text_x = start_x + icon_to_text_total

            _paste_icon(canvas, SOCIAL_ICON, start_x, icon_y, size=_ss(24), opacity=255)

            draw.text(
                (text_x, FRAME_H - _sy(120)),
                social_text,
                font=social_font,
                fill=(107, 114, 128, 255)
            )

    output_path = os.path.join(TEMP_RENDER_DIR, f"task_end_{frame_index}.png").replace("\\", "/")
    canvas.convert("RGB").save(output_path, quality=95)
    return output_path


# ---------------------------------------------------------
# FFMPEG BUILDERS
# ---------------------------------------------------------

def _build_video_from_frames(
    frame_paths: List[str],
    frame_durations: List[float],
    total_video_duration: float,
    voice_audio: Optional[str],
    voice_volume: float,
    music_audio: Optional[str],
    music_volume: float,
    output_path: str
):
    cmd = ["ffmpeg", "-y"]

    for i, frame_path in enumerate(frame_paths):
        duration = frame_durations[i]
        padded_duration = duration + (TRANSITION_DURATION if i < len(frame_paths) - 1 else 0.0)
        cmd += [
            "-loop", "1",
            "-t", str(padded_duration),
            "-i", frame_path
        ]

    video_input_count = len(frame_paths)

    voice_index = None
    music_index = None

    if voice_audio and os.path.exists(voice_audio):
        voice_index = video_input_count
        cmd += ["-i", voice_audio]

    if music_audio and os.path.exists(music_audio):
        music_index = video_input_count + (1 if voice_index is not None else 0)
        cmd += ["-stream_loop", "-1", "-i", music_audio]

    filters = []

    for i in range(video_input_count):
        filters.append(
            f"[{i}:v]scale={FRAME_W}:{FRAME_H},format=yuv420p,setsar=1[v{i}]"
        )

    current = "[v0]"
    offset_accum = frame_durations[0]

    for i in range(1, video_input_count):
        out = f"[vx{i}]"
        filters.append(
            f"{current}[v{i}]xfade=transition=fade:duration={TRANSITION_DURATION}:offset={offset_accum}{out}"
        )
        current = out
        offset_accum += frame_durations[i]

    video_label = current

    audio_filter = None

    if voice_index is not None and music_index is not None:
        audio_filter = (
            f"[{voice_index}:a]volume={voice_volume},apad=pad_dur={END_SCREEN_DURATION}[a_voice];"
            f"[{music_index}:a]volume={music_volume}[a_music];"
            f"[a_voice][a_music]amix=inputs=2:duration=longest:dropout_transition=2,"
            f"atrim=0:{total_video_duration},aresample=async=1[a]"
        )

    elif voice_index is not None:
        audio_filter = (
            f"[{voice_index}:a]volume={voice_volume},apad=pad_dur={END_SCREEN_DURATION},"
            f"atrim=0:{total_video_duration},aresample=async=1[a]"
        )

    elif music_index is not None:
        audio_filter = (
            f"[{music_index}:a]volume={music_volume},"
            f"atrim=0:{total_video_duration},aresample=async=1[a]"
        )

    filter_complex = ";".join(filters)
    if audio_filter:
        filter_complex += ";" + audio_filter

    cmd += [
        "-filter_complex", filter_complex,
        "-map", video_label
    ]

    if audio_filter:
        cmd += ["-map", "[a]"]

    cmd += [
        "-t", str(total_video_duration),
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "18",
        "-pix_fmt", "yuv420p"
    ]

    if audio_filter:
        cmd += ["-c:a", "aac", "-b:a", "192k"]

    cmd += [output_path]

    print("\n================ FFMPEG COMMAND START ================\n")
    print(" ".join(cmd))
    print("\n================ FFMPEG COMMAND END ==================\n")

    _run_subprocess(cmd)


# ---------------------------------------------------------
# MAIN RENDER FUNCTION
# ---------------------------------------------------------

def render_video(task_id: int, payload: dict, db: Session):
    variant_id = payload.get("variant_id")
    if not variant_id:
        raise Exception("variant_id is required in payload")

    raw_slides = payload.get("slides", []) or []

    slide_paths = []
    for s in raw_slides:
        slide_url = s.get("url")
        local_slide = _normalize_local_path(slide_url)
        if local_slide:
            slide_paths.append(local_slide)

    if not slide_paths:
        raise Exception("No slides")

    voice = payload.get("voice", {}) or {}
    music = payload.get("music", {}) or {}
    qr = payload.get("qr", {}) or {}
    banner = payload.get("banner", {}) or {}
    end_screen = payload.get("end_screen", {}) or {}

    banner_enabled = bool(banner.get("enabled"))
    banner_data = banner.get("data", {}) or {}

    end_enabled = bool(end_screen.get("enabled"))

    qr_enabled = bool(qr.get("enabled")) and bool(qr.get("url"))
    qr_url = qr.get("url")

    voice_enabled = bool(voice.get("enabled"))
    music_enabled = bool(music.get("enabled"))

    voice_audio = _normalize_local_path(voice.get("audio")) if voice_enabled else None
    music_audio = _normalize_local_path(music.get("audio")) if music_enabled else None

    voice_volume = float(voice.get("volume", 0.88))
    music_volume = float(music.get("volume", 0.12))

    voice_duration = _get_audio_duration(voice_audio) if voice_audio else None

    if voice_duration and voice_duration > 0:
        slide_duration = max(1.8, voice_duration / len(slide_paths))
        slideshow_duration = slide_duration * len(slide_paths)
    else:
        slide_duration = 2.5
        slideshow_duration = slide_duration * len(slide_paths)

    total_video_duration = slideshow_duration + (END_SCREEN_DURATION if end_enabled else 0.0)

    temp_output_path = get_video_output_path(task_id).replace("\\", "/")

    _ensure_dir(TEMP_RENDER_DIR)

    frame_paths = []
    frame_durations = []

    for i, slide_path in enumerate(slide_paths):
        frame_path = _build_slide_frame(
            slide_path=slide_path,
            banner_data=banner_data,
            banner_enabled=banner_enabled,
            qr_enabled=qr_enabled,
            qr_url=qr_url,
            frame_index=i
        )
        frame_paths.append(frame_path)
        frame_durations.append(slide_duration)

    if end_enabled:
        end_frame = _build_end_screen_frame(end_screen=end_screen, frame_index=len(frame_paths))
        if end_frame:
            frame_paths.append(end_frame)
            frame_durations.append(END_SCREEN_DURATION)

    _build_video_from_frames(
        frame_paths=frame_paths,
        frame_durations=frame_durations,
        total_video_duration=total_video_duration,
        voice_audio=voice_audio,
        voice_volume=voice_volume,
        music_audio=music_audio,
        music_volume=music_volume,
        output_path=temp_output_path
    )

    final_video_ref = store_rendered_video(
        local_temp_video_path=temp_output_path,
        task_id=task_id,
        variant_id=variant_id
    )

    db.add(
        AdVideo(
            variant_id=variant_id,
            video_path=final_video_ref
        )
    )
    db.commit()

    return final_video_ref