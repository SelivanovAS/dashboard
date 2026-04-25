"""Одноразовый скрипт: генерирует icon-192.png и icon-512.png для PWA.
Зелёный круг #21a038 на белом фоне (maskable safe zone — белый padding 10%),
в центре крупная белая буква «С». Шрифт — системный (PIL default или DejaVu Sans Bold).

Запускать из корня репо:  python3 scripts/_gen_pwa_icons.py
"""
import os
from PIL import Image, ImageDraw, ImageFont

BG_WHITE = (255, 255, 255, 255)
SBER_GREEN = (33, 160, 56, 255)  # #21a038
TEXT_WHITE = (255, 255, 255, 255)


def find_bold_font(size: int) -> ImageFont.FreeTypeFont:
    """Ищет жирный системный шрифт с поддержкой кириллицы."""
    candidates = [
        # macOS
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/Library/Fonts/Arial Bold.ttf",
        # Linux
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    return ImageFont.load_default()


def render_icon(size: int, out_path: str) -> None:
    img = Image.new("RGBA", (size, size), BG_WHITE)
    draw = ImageDraw.Draw(img)

    # Maskable safe zone: важный контент в центральных 80%.
    # Круг занимает 80% от размера → padding 10% с каждой стороны.
    pad = int(size * 0.10)
    draw.ellipse([pad, pad, size - pad, size - pad], fill=SBER_GREEN)

    # Буква «С» — белая, ~55% от размера круга.
    font_size = int(size * 0.55)
    font = find_bold_font(font_size)
    text = "С"

    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    # Центрируем с учётом смещения bbox (особенно по вертикали).
    tx = (size - tw) / 2 - bbox[0]
    ty = (size - th) / 2 - bbox[1]
    draw.text((tx, ty), text, fill=TEXT_WHITE, font=font)

    img.save(out_path, "PNG", optimize=True)
    print(f"  → {out_path} ({size}×{size})")


def main() -> None:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for size, name in [(192, "icon-192.png"), (512, "icon-512.png")]:
        render_icon(size, os.path.join(repo_root, name))


if __name__ == "__main__":
    main()
