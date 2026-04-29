"""Иконки PWA — минимализм iOS-стиля: squircle Sber green + крупный «§».

§ — общеизвестный юридический символ (параграф закона), идеально
читается даже в 48×48. Squircle (superellipse) повторяет форму
иконок iOS.

Запуск: python3 scripts/generate_icon.py
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

SBER_GREEN = (33, 160, 56, 255)
WHITE = (255, 255, 255, 255)
TRANSPARENT = (0, 0, 0, 0)

# Мягкий вертикальный градиент в одном тоне: светлее сверху → темнее снизу
GRADIENT_TOP = (46, 184, 75, 255)
GRADIENT_BOTTOM = (24, 138, 47, 255)

REPO_ROOT = Path(__file__).resolve().parent.parent

SIZES: dict[str, int] = {
    "icon-512.png": 512,
    "icon-192.png": 192,
    "icon-180.png": 180,
}

FONT_PATH = "/System/Library/Fonts/SFCompactRounded.ttf"
SUPERSAMPLE = 4  # рендерим в 4× и даунскейлим — антиалиасинг


def render_icon(target_size: int) -> Image.Image:
    big = target_size * SUPERSAMPLE
    canvas = Image.new("RGBA", (big, big), TRANSPARENT)
    draw = ImageDraw.Draw(canvas)

    # Squircle (iOS-стиль скруглённость ~22.5% от стороны) с вертикальным градиентом
    radius = int(big * 0.225)
    gradient = Image.new("RGBA", (big, big))
    gpx = gradient.load()
    for y in range(big):
        t = y / (big - 1)
        gpx[0, y] = tuple(
            int(GRADIENT_TOP[i] + (GRADIENT_BOTTOM[i] - GRADIENT_TOP[i]) * t)
            for i in range(4)
        )
    for y in range(big):
        c = gpx[0, y]
        for x in range(1, big):
            gpx[x, y] = c
    mask = Image.new("L", (big, big), 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, big - 1, big - 1), radius=radius, fill=255)
    canvas.paste(gradient, (0, 0), mask)
    draw = ImageDraw.Draw(canvas)

    # Символ § — крупно, по центру, белый
    font_size = int(big * 0.62)
    try:
        font = ImageFont.truetype(FONT_PATH, font_size)
    except OSError:
        font = ImageFont.load_default()

    glyph = "§"
    # Жирность эмулируем обводкой того же цвета, что и заливка
    # (stroke_width в 4×-разрешении = ~6% от размера символа).
    stroke = max(1, int(font_size * 0.035))
    bbox = draw.textbbox((0, 0), glyph, font=font, stroke_width=stroke)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    tx = (big - tw) / 2 - bbox[0]
    ty = (big - th) / 2 - bbox[1]
    draw.text(
        (tx, ty), glyph, font=font, fill=WHITE,
        stroke_width=stroke, stroke_fill=WHITE,
    )

    return canvas.resize((target_size, target_size), Image.LANCZOS)


def main() -> None:
    for filename, size in SIZES.items():
        out_path = REPO_ROOT / filename
        render_icon(size).save(out_path, format="PNG", optimize=True)
        print(f"  ✓ {filename} ({size}×{size})")


if __name__ == "__main__":
    main()
