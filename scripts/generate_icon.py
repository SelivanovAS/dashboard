"""Генератор иконок PWA для дашборда «Сбер Юрист».

Рисует молоток судьи (gavel) на зелёном круге Сбера и выгружает PNG
в трёх размерах для manifest.json и apple-touch-icon.

Запуск: python3 scripts/generate_icon.py
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw

SBER_GREEN = (33, 160, 56, 255)
WHITE = (255, 255, 255, 255)
TRANSPARENT = (0, 0, 0, 0)

REPO_ROOT = Path(__file__).resolve().parent.parent

SIZES: dict[str, int] = {
    "icon-512.png": 512,
    "icon-192.png": 192,
    "icon-180.png": 180,
}


def render_icon(size: int) -> Image.Image:
    """Рисует одну иконку заданного размера и возвращает её."""
    scale = size / 512.0
    img = Image.new("RGBA", (size, size), TRANSPARENT)
    draw = ImageDraw.Draw(img)

    # Зелёный круг во весь размер (для maskable safe-area задаём радиус 50%).
    draw.ellipse((0, 0, size - 1, size - 1), fill=SBER_GREEN)

    # Молоток (gavel) — две части:
    #   • боёк (цилиндр-параллелепипед, наклонён под 30° по часовой)
    #   • рукоять (узкий прямоугольник от центра вниз-вправо)
    # Подставка снизу — горизонтальная плита (sounding block).
    # Все геометрии нарисованы в координатах 512×512 и масштабируются.
    cx, cy = 256, 256
    s = scale  # короткий алиас

    # Подставка (sounding block) внизу
    plate_w = 340
    plate_h = 38
    plate_left = cx - plate_w // 2
    plate_top = 388
    draw.rounded_rectangle(
        (plate_left * s, plate_top * s, (plate_left + plate_w) * s, (plate_top + plate_h) * s),
        radius=12 * s,
        fill=WHITE,
    )

    # Молоток — собираем горизонтально на отдельном слое и потом поворачиваем,
    # чтобы боёк лёг наискось под 30°.
    hammer_layer = Image.new("RGBA", (size, size), TRANSPARENT)
    h_draw = ImageDraw.Draw(hammer_layer)

    pivot_x, pivot_y = cx, 372  # точка удара по подставке

    # Рукоять — от точки удара вверх-влево вдоль оси поворота
    handle_w = 64
    handle_h = 240
    handle_left = pivot_x - handle_w // 2
    handle_bottom = pivot_y
    handle_top = handle_bottom - handle_h
    h_draw.rounded_rectangle(
        (handle_left * s, handle_top * s, (handle_left + handle_w) * s, handle_bottom * s),
        radius=14 * s,
        fill=WHITE,
    )

    # Боёк — широкий блок поверх рукояти в её верхней трети
    head_w = 300
    head_h = 110
    head_cy = handle_top + 30
    head_left = cx - head_w // 2
    head_top = head_cy - head_h // 2
    h_draw.rounded_rectangle(
        (head_left * s, head_top * s, (head_left + head_w) * s, (head_top + head_h) * s),
        radius=18 * s,
        fill=WHITE,
    )
    # Декоративные кольца-обручи на бойке
    ring_w = 16
    for offset in (-head_w // 2 + 30, head_w // 2 - 30 - ring_w):
        rx = cx + offset
        h_draw.rectangle(
            (rx * s, head_top * s, (rx + ring_w) * s, (head_top + head_h) * s),
            fill=SBER_GREEN,
        )

    # Поворот молотка вокруг точки удара (имитируем замах)
    hammer_layer = hammer_layer.rotate(
        28, resample=Image.BICUBIC, center=(pivot_x * s, pivot_y * s)
    )
    img = Image.alpha_composite(img, hammer_layer)
    return img


def main() -> None:
    for filename, size in SIZES.items():
        out_path = REPO_ROOT / filename
        icon = render_icon(size)
        icon.save(out_path, format="PNG", optimize=True)
        print(f"  ✓ {filename} ({size}×{size})")


if __name__ == "__main__":
    main()
