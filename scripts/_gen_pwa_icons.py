"""Генератор иконок PWA: зелёная папка с весами правосудия.

Запускать из корня репо:  python3 scripts/_gen_pwa_icons.py
"""
import os
from PIL import Image, ImageDraw

BG = (255, 255, 255, 255)
GREEN = (33, 160, 56, 255)
WHITE = (255, 255, 255, 255)


def sc(v: float, size: int) -> int:
    return int(v * size / 1024)


def draw_icon(size: int) -> Image.Image:
    # Рисуем на 1024→scale, затем сжимаем (лучше anti-alias)
    SRC = 1024
    img = Image.new('RGBA', (SRC, SRC), BG)
    d = ImageDraw.Draw(img)

    def p(v): return sc(v, SRC)

    pad = p(80)
    tab_h = p(110)
    tab_w = p(320)
    body_top = pad + tab_h
    body_r = p(70)
    tab_r = p(45)

    # --- Папка ---
    # Тело
    d.rounded_rectangle([pad, body_top, SRC - pad, SRC - pad], radius=body_r, fill=GREEN)
    # Вкладка (tab): перекрывает верх тела, нижние скруглённые углы прячутся за телом
    d.rounded_rectangle([pad, pad, pad + tab_w, body_top + body_r], radius=tab_r, fill=GREEN)
    # Залить «щель» между вкладкой и телом
    d.rectangle([pad, body_top, pad + tab_w, body_top + body_r], fill=GREEN)

    # --- Весы правосудия (белые) ---
    cx = SRC // 2

    icon_top = body_top + p(70)
    icon_bot = SRC - pad - p(60)

    # Ножка/столп
    pw = p(30)
    pt = icon_top + p(55)
    pb = icon_bot - p(65)
    d.rectangle([cx - pw // 2, pt, cx + pw // 2, pb], fill=WHITE)

    # Шарик сверху столпа
    kr = p(28)
    d.ellipse([cx - kr, pt - kr, cx + kr, pt + kr], fill=WHITE)

    # Перекладина
    by = pt + p(55)
    bh = p(22)
    bspan = p(430)
    bx1, bx2 = cx - bspan // 2, cx + bspan // 2
    d.rectangle([bx1, by - bh // 2, bx2, by + bh // 2], fill=WHITE)

    # Шарики на концах перекладины
    ber = p(22)
    d.ellipse([bx1 - ber, by - ber, bx1 + ber, by + ber], fill=WHITE)
    d.ellipse([bx2 - ber, by - ber, bx2 + ber, by + ber], fill=WHITE)

    # Чаши: центры по X немного смещены внутрь
    bowl_cx_l = bx1 + p(15)
    bowl_cx_r = bx2 - p(15)
    chain_w = p(18)
    bowl_top_y = icon_bot - p(215)  # откуда начинается чаша
    bowl_bot_y = icon_bot - p(80)   # низ чаши

    # Цепи
    d.line([bx1, by, bowl_cx_l, bowl_top_y + p(30)], fill=WHITE, width=chain_w)
    d.line([bx2, by, bowl_cx_r, bowl_top_y + p(30)], fill=WHITE, width=chain_w)

    # Форма чаши: нижний полуэллипс + прямые боковые края
    bw = p(240)
    # Рисуем как заполненный эллипс в нижней половине + прямоугольник сверху
    # Левая чаша
    for bcx, bcy in [(bowl_cx_l, bowl_bot_y - p(60)), (bowl_cx_r, bowl_bot_y - p(60))]:
        beh = bowl_bot_y - bowl_top_y  # высота эллипса
        bew = bw
        # Заполненный эллипс
        d.ellipse([bcx - bew // 2, bowl_top_y, bcx + bew // 2, bowl_bot_y], fill=WHITE)
        # «Срезаем» верхнюю часть эллипса — рисуем зелёный прямоугольник поверх верхней части
        cut = int(beh * 0.55)
        d.rectangle([bcx - bew // 2 + p(8), bowl_top_y - p(5),
                     bcx + bew // 2 - p(8), bowl_top_y + cut], fill=GREEN)
        # Боковые стенки (тонкие белые прямоугольники)
        wall_w = p(22)
        d.rectangle([bcx - bew // 2, bowl_top_y + p(20),
                     bcx - bew // 2 + wall_w, bowl_top_y + cut + p(10)], fill=WHITE)
        d.rectangle([bcx + bew // 2 - wall_w, bowl_top_y + p(20),
                     bcx + bew // 2, bowl_top_y + cut + p(10)], fill=WHITE)

    # Основание
    base_w = p(275)
    base_h = p(42)
    base_y = icon_bot - p(35)
    d.rounded_rectangle(
        [cx - base_w // 2, base_y - base_h // 2,
         cx + base_w // 2, base_y + base_h // 2],
        radius=p(14), fill=WHITE
    )

    # Отрисовка финального изображения нужного размера
    return img.resize((size, size), Image.LANCZOS)


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for size, name in [(192, "icon-192.png"), (512, "icon-512.png")]:
        draw_icon(size).save(os.path.join(root, name), "PNG", optimize=True)
        print(f"  → {name} ({size}×{size})")


if __name__ == "__main__":
    main()
