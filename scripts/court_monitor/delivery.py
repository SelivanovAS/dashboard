# -*- coding: utf-8 -*-
"""Доставка: Telegram (send_telegram + split_message) и Web Push PWA
(send_web_push с ленивым импортом pywebpush; персонализация по watchlist
через _make_per_sub_callback / _filter_events_by_watchlist), журнал
последней push-рассылки, сервисные алерты (send_crash_alert),
итоговая сводка прогона (log_run_summary).

Новые дела (fi_new_cases / appeal_new_cases_csv) — общесистемный сигнал,
шлются всем подпискам; изменения и переходы — только по watchlist.
"""

from __future__ import annotations

import json
import os
import re
import time
import traceback
from datetime import datetime
from html import escape as html_escape

import requests

from court_monitor import config, ghlog
from court_monitor.config import log
from court_monitor.digest.postprocess import (
    _close_open_tags, _strip_orphan_close_tags,
)
from court_monitor.storage import save_json
from court_monitor.textutil import _bare_case_number

def _extract_paren_numbers(s) -> list[str]:
    """Достаёт номера из скобок hybrid-ID. `2-208/2026 (2-1148/2025;)` →
    `["2-1148/2025"]`. Зеркало одноимённой функции в worker.js и
    audit_watchlists.py."""
    m = re.search(r"\(([^)]+)\)", str(s or ""))
    if not m:
        return []
    return [
        b for b in (_bare_case_number(x) for x in re.split(r"[;,]", m.group(1)))
        if b
    ]


def _build_watchlist_alias_indexes(
    cases: list[dict],
) -> tuple[dict[str, str], dict[str, set[str]]]:
    """По списку дел строит (alias_to_canonical, canonical_to_aliases) для
    расширения watchlist при фильтрации push-событий.

    Канонический ID = `_bare_case_number(c.id)`. Алиасами считаются bare-формы:
    `c.id`, `first_instance.case_number`, `appeal.case_number`,
    `cassation.case_number`, `cassation.cassation_number`, а также все
    предыдущие номера из скобок hybrid-ID (`(2-1148/2025;)`).

    Возвращает две карты:
    · alias_to_canonical — по любому known-bare-номеру даёт канон. id;
    · canonical_to_aliases — по канон. id даёт все его алиасы (set).

    Юрист звёздил `8Г-5513/2026` → bare = `8Г-5513/2026` → канон. =
    `2-3760/2025` → expanded set содержит и кассац., и FI-номер.
    """
    alias_to_canonical: dict[str, str] = {}
    canonical_to_aliases: dict[str, set[str]] = {}
    for c in cases:
        canonical = _bare_case_number(c.get("id", ""))
        if not canonical:
            continue
        fi = c.get("first_instance") or {}
        ap = c.get("appeal") or {}
        ca = c.get("cassation") or {}
        aliases: set[str] = set()
        for raw in (
            c.get("id"),
            fi.get("case_number"),
            fi.get("material_number"),  # М-предок (Этап 3)
            ap.get("case_number"),
            ca.get("case_number"),
            ca.get("cassation_number"),
        ):
            bare = _bare_case_number(raw)
            if bare:
                aliases.add(bare)
        for prev in _extract_paren_numbers(c.get("id", "")):
            aliases.add(prev)
        for a in aliases:
            # Первая встретившаяся канон. побеждает — как в worker.js.
            if a not in alias_to_canonical:
                alias_to_canonical[a] = canonical
        canonical_to_aliases.setdefault(canonical, set()).update(aliases)
    return alias_to_canonical, canonical_to_aliases


def _expand_watchlist_via_aliases(
    wl_raw: list[str],
    alias_to_canonical: dict[str, str],
    canonical_to_aliases: dict[str, set[str]],
) -> set[str]:
    """`{"8Г-5513/2026"}` → `{"8Г-5513/2026", "2-3760/2025"}` если канон.
    запись найдена в alias-картe. Звезда на любом алиасе расширяется во все
    известные номера того же дела."""
    wl_bare = {_bare_case_number(x) for x in (wl_raw or []) if _bare_case_number(x)}
    expanded = set(wl_bare)
    for b in wl_bare:
        cid = alias_to_canonical.get(b)
        if cid:
            expanded |= canonical_to_aliases.get(cid, set())
    return expanded


def _filter_events_by_watchlist(
    watchlist: set[str],
    *,
    fi_new_cases: list[dict],
    fi_changes: list[dict],
    stage_transitions: list[dict],
    appeal_new_cases_csv: list[dict],
    changes: list[dict],
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
) -> dict:
    """Отфильтровать списки событий по идентификаторам дел в watchlist.

    Идентификатор в watchlist (после `_expand_watchlist_via_aliases`) = set
    bare-номеров: c.id, fi.case_number, appeal.case_number,
    cassation.case_number, hybrid-предки. Поля события (`ch.get("case")`)
    нормализуются через `_bare_case_number` — это закрывает hybrid-форму
    `fi.case_number = "2-208/2026 (2-1148/2025;)"` (она тоже сравнивается в
    bare-форме `2-208/2026`).

    Маппинг полей:
    · changes (apel)        → ch["case"] (номер апел. дела)
    · fi_changes            → ch["case"] (= fi.case_number, может быть hybrid)
    · cass_changes          → ch["case"] (= номер 1-й инст., канон. id)
    · fi_new_cases          → c["id"]            (НЕ фильтруем, общесистемно)
    · appeal_new_cases_csv  → c["Номер дела"]    (НЕ фильтруем, общесистемно)
    · cass_discovered       → c["id"]            (НЕ фильтруем, общесистемно)
    · stage_transitions     → fi_case_number ИЛИ appeal_case_number
      (юрист может отслеживать дело по любому из них).
    """
    return {
        "fi_new_cases": list(fi_new_cases or []),
        "fi_changes": [
            ch for ch in (fi_changes or [])
            if _bare_case_number(ch.get("case")) in watchlist
        ],
        "stage_transitions": [
            t for t in (stage_transitions or [])
            if _bare_case_number(t.get("fi_case_number")) in watchlist
            or _bare_case_number(t.get("appeal_case_number")) in watchlist
        ],
        "appeal_new_cases_csv": list(appeal_new_cases_csv or []),
        "changes": [
            ch for ch in (changes or [])
            if _bare_case_number(ch.get("case")) in watchlist
        ],
        "cass_changes": [
            ch for ch in (cass_changes or [])
            if _bare_case_number(ch.get("case")) in watchlist
        ],
        "cass_discovered": list(cass_discovered or []),
    }


def _drop_dead_subscription(endpoint: str) -> None:
    """Удалить мёртвую подписку из KV через `/unsubscribe` на Worker.

    Вызывается автоматически после WebPushException 410/404. Тихая —
    любая ошибка логируется и не валит прогон, очистка best-effort.
    """
    if not config.PUSH_WORKER_URL or not config.PUSH_SECRET or not endpoint:
        return
    try:
        r = requests.post(
            f"{config.PUSH_WORKER_URL}/unsubscribe",
            headers={
                "Authorization": f"Bearer {config.PUSH_SECRET}",
                "Content-Type": "application/json",
            },
            json={"endpoint": endpoint},
            timeout=10,
        )
        if r.ok:
            log.info(f"Web Push: мёртвая подписка удалена из KV ({endpoint[:60]})")
        else:
            log.warning(
                f"Web Push: /unsubscribe вернул {r.status_code} для {endpoint[:60]}"
            )
    except Exception as exc:
        log.warning(f"Web Push: не удалось удалить подписку: {exc}")


def _canonicalize_one_watchlist(
    wl_raw: list, alias_to_canonical: dict[str, str],
) -> tuple[list[str], list[tuple[str, str]]]:
    """Чистая функция: нормализует список номеров через alias_to_canonical.

    Возвращает (canonical_list, replaced) — где canonical_list это
    дедуплицированный набор канон. ID (плюс «неразрешённые» bare-номера —
    М-материалы, truly-orphan), а replaced — пары (bare, канон) для лога.
    """
    canon_list: list[str] = []
    replaced: list[tuple[str, str]] = []
    for x in wl_raw or []:
        bare = _bare_case_number(x)
        if not bare:
            continue
        canonical = alias_to_canonical.get(bare, bare)
        canon_list.append(canonical)
        if canonical != bare:
            replaced.append((bare, canonical))
    return list(dict.fromkeys(canon_list)), replaced


def canonicalize_kv_watchlists(alias_to_canonical: dict[str, str]) -> None:
    """Канонизация watchlist'ов в KV через POST /admin/watchlist.

    Для каждой подписки: если в watchlist есть апел./касс./hybrid номера,
    заменяем их на канон. FI-ID. М-материалы и truly-orphan номера
    остаются как есть (нет соответствия в alias_to_canonical).

    Зачем: после Этапа 4a фильтр умеет расширять алиасы в runtime, но KV
    остаётся «грязной» — со временем накапливаются устаревшие апел./касс.
    звёзды. Канонизация постепенно вычищает их.

    Запускать только в живом кроне (main_json), НЕ в replay/test режимах
    — тестовые прогоны не должны менять состояние KV.

    Список подписок берём через `/subscriptions` (тот же endpoint, что
    send_web_push, auth Bearer PUSH_SECRET). Обновляем через
    `/admin/watchlist?secret=$OWNER_SECRET`.
    """
    if not config.PUSH_WORKER_URL or not config.PUSH_SECRET:
        log.info("Канонизация watchlist'ов: переменные не настроены, пропуск")
        return
    secret = os.environ.get("OWNER_SECRET", "")
    if not secret:
        log.warning(
            "Канонизация watchlist'ов: нет OWNER_SECRET в env, пропуск"
        )
        return
    try:
        r = requests.get(
            f"{config.PUSH_WORKER_URL}/subscriptions",
            headers={"Authorization": f"Bearer {config.PUSH_SECRET}"},
            timeout=10,
        )
        if not r.ok:
            log.warning(
                f"Канонизация: GET /subscriptions вернул {r.status_code}"
            )
            return
        subs = r.json() or []
    except Exception as exc:
        log.warning(f"Канонизация: GET /subscriptions упал: {exc}")
        return

    updated = 0
    for sub in subs:
        endpoint = sub.get("endpoint") or ""
        wl_raw = sub.get("watchlist") or []
        if not endpoint or not isinstance(wl_raw, list) or not wl_raw:
            continue

        canon_list, replaced = _canonicalize_one_watchlist(wl_raw, alias_to_canonical)

        # Сравниваем с тем, что юрист отправил, в bare-форме с дедупом.
        # Если разницы нет — не дёргаем Worker зря.
        raw_normalised = list(dict.fromkeys(
            b for b in (_bare_case_number(x) for x in wl_raw) if b
        ))
        if canon_list == raw_normalised:
            continue

        try:
            resp = requests.post(
                f"{config.PUSH_WORKER_URL}/admin/watchlist",
                params={"secret": secret},
                json={"endpoint": endpoint, "watchlist": canon_list},
                timeout=10,
            )
            if resp.ok:
                label = sub.get("label") or "?"
                ep_short = endpoint[-32:]
                log.info(
                    f"Канонизация watchlist'а ({label} …{ep_short}): "
                    f"{len(wl_raw)} → {len(canon_list)} дел, "
                    f"заменено алиасов: {len(replaced)}"
                )
                updated += 1
            else:
                log.warning(
                    f"Канонизация: POST /admin/watchlist {resp.status_code} "
                    f"для …{endpoint[-32:]}"
                )
        except Exception as exc:
            log.warning(f"Канонизация: POST упал: {exc}")

    if updated:
        log.info(f"Канонизация watchlist'ов: обновлено {updated} подписок")
    else:
        log.info("Канонизация watchlist'ов: всё уже канон., обновлений нет")


def _make_per_sub_callback(
    *,
    cases: list[dict],
    fi_new_cases: list[dict],
    fi_changes: list[dict],
    changes: list[dict],
    stage_transitions: list[dict],
    appeal_new_cases_csv: list[dict],
    push_summary: str,
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
):
    """Фабрика callback'а для `send_web_push(per_subscriber=...)`.

    `cases` — список активных + архивных дел; нужен для построения alias-
    индексов. Юрист может звёздить дело по любому из 3-4 номеров (FI,
    апел., касс., hybrid-предок), а `_filter_events_by_watchlist` шлёт
    события по канон. ID. Без расширения watchlist через алиасы push'и
    не долетают по таким звёздам.

    Логика отправки push с учётом подписки на дела:
    · watchlist пуст и событий вообще нет → None (ничего не шлём).
    · watchlist пуст, но есть любые события (новые дела ИЛИ изменения ИЛИ
      переходы стадий) → общий push с push_summary, без фильтрации.
    · watchlist непуст → персональный push: `_filter_events_by_watchlist`
      пропускает все новые дела целиком + только изменения по своим делам.
      Заголовок «Мониторинг дел — твои дела», click_url с `?mine=1`.
    · watchlist непуст, но и своих изменений, и новых дел нет → None.

    Используется в main_json (живой крон), main_replay_last,
    main_push_last_digest — чтобы тестовые режимы вели себя как боевой.
    """
    cass_changes = cass_changes or []
    cass_discovered = cass_discovered or []

    # Карты алиасов строим один раз на крон-прогон. Стоимость — ~150 записей,
    # копейки. Дальше каждая подписка дёшево расширяется через эти карты.
    alias_to_canonical, canonical_to_aliases = _build_watchlist_alias_indexes(
        cases or []
    )

    def _per_sub(sub: dict):
        wl_raw = sub.get("watchlist") or []
        wl = _expand_watchlist_via_aliases(
            wl_raw, alias_to_canonical, canonical_to_aliases
        )

        if not wl:
            # Пустой watchlist — общесистемный push при любых событиях.
            total_global = (
                len(fi_new_cases) + len(appeal_new_cases_csv) + len(cass_discovered)
                + len(fi_changes) + len(changes) + len(cass_changes)
                + len(stage_transitions)
            )
            if total_global == 0:
                return None
            return (
                "Мониторинг дел — обновление",
                push_summary,
                "/sberbank_dashboard.html?digest=open",
            )

        f = _filter_events_by_watchlist(
            wl,
            fi_new_cases=fi_new_cases,
            fi_changes=fi_changes,
            stage_transitions=stage_transitions,
            appeal_new_cases_csv=appeal_new_cases_csv,
            changes=changes,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )
        n_new = (
            len(f["fi_new_cases"])
            + len(f["appeal_new_cases_csv"])
            + len(f.get("cass_discovered") or [])
        )
        n_chg = (
            len(f["fi_changes"]) + len(f["changes"])
            + len(f.get("cass_changes") or [])
        )
        n_st = len(f["stage_transitions"])
        if n_new + n_chg + n_st == 0:
            return None
        # Перечень: до 3 номеров, остаток сворачиваем в «и ещё N».
        ids: list[str] = []
        for c in f["fi_new_cases"]:
            ids.append((c.get("id") or "").strip())
        for c in f["appeal_new_cases_csv"]:
            ids.append((c.get("Номер дела") or "").strip())
        for c in (f.get("cass_discovered") or []):
            ids.append((c.get("id") or "").strip())
        for ch in f["fi_changes"]:
            ids.append((ch.get("case") or "").strip())
        for ch in f["changes"]:
            ids.append((ch.get("case") or "").strip())
        for ch in (f.get("cass_changes") or []):
            ids.append((ch.get("case") or "").strip())
        for t in f["stage_transitions"]:
            ids.append(
                (t.get("appeal_case_number") or t.get("fi_case_number") or "").strip()
            )
        ids_uniq: list[str] = []
        seen: set[str] = set()
        for x in ids:
            if x and x not in seen:
                seen.add(x)
                ids_uniq.append(x)
        head = ", ".join(ids_uniq[:3])
        tail = f" и ещё {len(ids_uniq) - 3}" if len(ids_uniq) > 3 else ""
        total = n_new + n_chg + n_st
        body = (
            f"Изменения по {len(ids_uniq)} "
            f"{'делу' if len(ids_uniq) == 1 else 'делам'}: {head}{tail}"
            + (f" · всего событий: {total}" if total > len(ids_uniq) else "")
        )
        return (
            "Мониторинг дел — твои дела",
            body,
            "/sberbank_dashboard.html?digest=open&mine=1",
        )

    return _per_sub


def send_web_push(
    title: str,
    body: str,
    *,
    click_url: str | None = None,
    owner_only: bool = False,
    per_subscriber=None,
) -> None:
    """Отправить Web Push PWA-подписчикам через Cloudflare Worker + pywebpush.

    `click_url` — относительный или абсолютный URL, который Service Worker откроет
    по клику на уведомление. По умолчанию открывается дашборд с раскрытым блоком
    последнего дайджеста.

    `owner_only=True` — слать только устройствам, помеченным владельческими
    (через POST /mark-owner). Используется в тестовых режимах (`--replay-last`,
    `--digest-only`), чтобы пробные пуши не улетали коллегам.

    `per_subscriber` — опциональный callable(sub_dict) → (title, body, click_url)
    либо None. Если задан, push-payload строится индивидуально для каждой
    подписки. Возврат None означает «для этой подписки нет персональных
    событий — пропустить». Используется для персонализации основного крона
    по watchlist подписчика.
    """
    if not config.PUSH_WORKER_URL or not config.PUSH_SECRET or not config.VAPID_PRIVATE_KEY:
        log.info("Web Push: переменные не настроены, пропуск")
        return
    try:
        # Получаем список подписок от Worker
        list_url = f"{config.PUSH_WORKER_URL}/subscriptions"
        if owner_only:
            list_url += "?role=owner"
        r = requests.get(
            list_url,
            headers={"Authorization": f"Bearer {config.PUSH_SECRET}"},
            timeout=10,
        )
        if not r.ok:
            log.warning(f"Web Push: не удалось получить подписки: {r.status_code}")
            return
        subscriptions = r.json()
        if not subscriptions:
            scope = "владельческих" if owner_only else ""
            log.info(f"Web Push: нет {scope}подписчиков".replace("  ", " ").strip())
            return
        log.info(
            f"Web Push: отправляю {len(subscriptions)} "
            f"{'владельческим ' if owner_only else ''}подписчикам"
        )

        import warnings as _w
        _w.filterwarnings("ignore")
        from pywebpush import webpush, WebPushException  # noqa: PLC0415
        from py_vapid import Vapid  # noqa: PLC0415

        # pywebpush.from_string не понимает PEM-строку из env (баг py_vapid 1.9.x);
        # явно создаём Vapid из bytes и передаём объект.
        vapid = Vapid.from_pem(config.VAPID_PRIVATE_KEY.encode())

        default_url = click_url or "/sberbank_dashboard.html?digest=open"
        ok_count = 0
        skipped = 0
        n_general = 0
        n_personal = 0
        # Журнал отправленных payload'ов — потом сохраним в
        # data/last_personal_pushes.json для админки.
        dump_items: list[dict] = []
        for sub in subscriptions:
            ep_full = sub.get("endpoint") or ""
            ep_short = ep_full[-32:] if ep_full else "?"
            wl_raw = sub.get("watchlist") or []
            wl_size = len(wl_raw) if isinstance(wl_raw, list) else 0
            is_owner = bool(sub.get("is_owner"))
            if per_subscriber is not None:
                personalised = per_subscriber(sub)
                if personalised is None:
                    skipped += 1
                    log.info(
                        f"Web Push: ⊘ skip ({'owner' if is_owner else 'user'}, "
                        f"watchlist={wl_size}) …{ep_short}"
                    )
                    dump_items.append({
                        "endpoint": ep_full,
                        "endpoint_tail": ep_short,
                        "is_owner": is_owner,
                        "watchlist_size": wl_size,
                        "watchlist": list(wl_raw) if isinstance(wl_raw, list) else [],
                        "variant": "skip",
                        "title": None,
                        "body": None,
                        "click_url": None,
                    })
                    continue
                p_title, p_body, p_url = personalised
                variant = (
                    "personal" if "твои дела" in (p_title or "")
                    else "general"
                )
                if variant == "personal":
                    n_personal += 1
                else:
                    n_general += 1
                log.info(
                    f"Web Push: → {variant} "
                    f"({'owner' if is_owner else 'user'}, watchlist={wl_size}) "
                    f"…{ep_short}"
                )
                payload = json.dumps(
                    {
                        "title": p_title,
                        "body": p_body,
                        "data": {"url": p_url or default_url},
                    },
                    ensure_ascii=False,
                )
                dump_items.append({
                    "endpoint": ep_full,
                    "endpoint_tail": ep_short,
                    "is_owner": is_owner,
                    "watchlist_size": wl_size,
                    "watchlist": list(wl_raw) if isinstance(wl_raw, list) else [],
                    "variant": variant,
                    "title": p_title,
                    "body": p_body,
                    "click_url": p_url or default_url,
                })
            else:
                payload = json.dumps(
                    {"title": title, "body": body, "data": {"url": default_url}},
                    ensure_ascii=False,
                )
                dump_items.append({
                    "endpoint": ep_full,
                    "endpoint_tail": ep_short,
                    "is_owner": is_owner,
                    "watchlist_size": wl_size,
                    "watchlist": list(wl_raw) if isinstance(wl_raw, list) else [],
                    "variant": "broadcast",
                    "title": title,
                    "body": body,
                    "click_url": default_url,
                })
            try:
                webpush(
                    subscription_info=sub,
                    data=payload,
                    vapid_private_key=vapid,
                    vapid_claims={"sub": config.VAPID_SUB_EMAIL},
                    ttl=43200,  # 12 часов: push-сервис держит сообщение,
                                # пока устройство не выйдет в сеть
                )
                ok_count += 1
                config.METRICS["push_sent"] += 1
            except WebPushException as exc:
                config.METRICS["push_failed"] += 1
                ep_full = sub.get("endpoint") or ""
                ep_short = ep_full[:60] or "?"
                log.warning(f"Web Push: ошибка для {ep_short}: {exc}")
                # Автоочистка: 410 Gone и 404 Not Found — это «подписка
                # мертва навсегда» (RFC 8030). Удаляем её из KV, чтобы не
                # тащить балласт каждый прогон.
                resp = getattr(exc, "response", None)
                status = getattr(resp, "status_code", None) if resp is not None else None
                if status in (404, 410) and ep_full:
                    _drop_dead_subscription(ep_full)
        suffix = f", пропущено по watchlist: {skipped}" if skipped else ""
        if per_subscriber is not None:
            suffix += f"; персональных: {n_personal}, общих: {n_general}"
        log.info(f"Web Push: отправлено {ok_count}/{len(subscriptions)}{suffix}")
        # Сохраняем журнал последней рассылки — админка читает этот файл,
        # чтобы показать «что получила каждая подписка». Перезаписывается
        # на каждом прогоне (только последняя рассылка, без истории).
        try:
            save_json({
                "version": 1,
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "title_default": title,
                "body_default": body,
                "owner_only": owner_only,
                "items": dump_items,
            }, config.LAST_PERSONAL_PUSHES_PATH)
        except Exception as exc:
            log.warning(f"Web Push: не удалось сохранить журнал push: {exc}")
    except Exception as exc:
        log.error(f"Web Push: исключение: {exc}")


def send_telegram(text: str):
    """Отправить сообщение в Telegram (HTML-формат)."""
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        log.warning("Telegram не настроен, сообщение не отправлено")
        preview = "\n".join(text.splitlines()[:3])
        log.info(f"Сообщение ({len(text)} символов), начало:\n{preview}")
        log.debug(f"Сообщение целиком:\n{text}")
        return

    # Разбиваем на части если превышен лимит
    parts = split_message(text, config.TELEGRAM_MSG_LIMIT)

    for i, part in enumerate(parts):
        try:
            # Финальная проверка: закрыть незакрытые теги
            part = _close_open_tags(part)
            r = requests.post(
                f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": config.TELEGRAM_CHAT_ID,
                    "text": part,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=30,
            )
            if r.ok:
                config.METRICS["telegram_sent"] += 1
                log.info(f"Telegram: сообщение {i + 1}/{len(parts)} отправлено")
            else:
                log.error(f"Telegram ошибка: {r.status_code} {r.text}")
                # Пробуем без разметки если не прошло
                plain = re.sub(r'<[^>]+>', '', part)
                r2 = requests.post(
                    f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": config.TELEGRAM_CHAT_ID,
                        "text": plain,
                        "disable_web_page_preview": True,
                    },
                    timeout=30,
                )
                if r2.ok:
                    config.METRICS["telegram_sent"] += 1
                    log.info("Telegram: отправлено без разметки")
                else:
                    config.METRICS["telegram_failed"] += 1
                    log.error(f"Telegram повторная ошибка: {r2.text}")

            # Пауза между частями
            if i < len(parts) - 1:
                time.sleep(1)

        except Exception as e:
            log.error(f"Telegram исключение: {e}")


def split_message(text: str, limit: int = 4096) -> list[str]:
    """Разбить сообщение на части по лимиту, не разрывая строки и HTML-теги."""
    if len(text) <= limit:
        return [text]

    parts = []
    while text:
        if len(text) <= limit:
            parts.append(_close_open_tags(text))
            break

        # Ищем точку разреза — двойной перенос (между секциями)
        cut = text[:limit - 50]  # запас для закрытия тегов
        split_pos = cut.rfind("\n\n")
        if split_pos < limit // 2:
            split_pos = cut.rfind("\n")
        if split_pos < limit // 3:
            split_pos = limit - 60

        part = text[:split_pos].rstrip()
        part = _close_open_tags(part)
        parts.append(part)

        text = text[split_pos:].lstrip("\n")
        text = _strip_orphan_close_tags(text)

    return parts


# ── Run summary ──────────────────────────────────────────────────────────────

def _format_timings(timings: dict[str, float]) -> str:
    """Форматирует словарь этап→секунды в короткую строку.

    Этапы — в порядке конвейера прогона (а не вставки в dict), total — всегда
    последним: так строка читается как хронология.
    """
    order = [
        "load_csv", "load_json",              # загрузка (legacy CSV / JSON)
        "search", "appeal_new", "first_instance",   # поиск новых дел
        "cards_update", "appeal_update", "fi_update",  # обновление карточек
        "cassation", "cassation_refresh",
        "digest", "telegram", "save",
    ]
    seen = set(order) | {"total"}
    known = [(k, timings[k]) for k in order if k in timings]
    extra = [(k, v) for k, v in timings.items() if k not in seen]
    tail = [("total", timings["total"])] if "total" in timings else []
    return " | ".join(f"{k} {v:.1f}s" for k, v in known + extra + tail)


# Русские подписи для ключей extras итоговой сводки. Сами ключи в коде
# вызывающих (runs.py) не трогаем — они исторические; переводим при печати.
# Неизвестный ключ печатается как есть.
_EXTRAS_RU = {
    "FI courts": "Судов 1 инст.",
    "FI new": "Новых 1 инст.",
    "FI updated": "Обновлено 1 инст.",
    "FI changes": "Изменений 1 инст.",
    "FI parse": "Парсинг 1 инст.",
    "FI skip": "Пропусков 1 инст.",
    "FI force": "Форс-парс 1 инст.",
    "Stage transitions": "Переходов стадий",
    "Appeal new": "Новых апел.",
    "Appeal changes": "Изменений апел.",
    "Appeal parse": "Парсинг апел.",
    "Appeal skip": "Пропусков апел.",
    "Appeal force": "Форс-парс апел.",
    "Cassation parse": "Парсинг касс.",
    "Cassation skip": "Пропусков касс.",
    "Cassation force": "Форс-парс касс.",
    "JSON total": "Всего дел в JSON",
    # legacy main() (CSV-режим апелляции)
    "Cases checked": "Проверено дел",
    "New": "Новых",
    "Changes": "Изменений",
    "Active after": "Активных после",
    "Archived moved": "В архив",
}


def log_run_summary(
    mode: str,
    timings: dict[str, float],
    extras: dict[str, object] | None = None,
) -> None:
    """
    Печатает итоговый блок метрик в лог и (если переменная установлена)
    в $GITHUB_STEP_SUMMARY — так он виден прямо в UI GitHub Actions.
    """
    # Закрыть группу последней фазы (log_phase в runs.py), чтобы сводка
    # была видна развёрнутой; вне GitHub Actions / вне группы — no-op.
    ghlog.end_group()
    extras = extras or {}
    req_line = (
        f"Запросы: {config.METRICS['requests_ok']} ок / "
        f"{config.METRICS['requests_failed']} сбоев"
    )
    if config.METRICS["requests_retried"]:
        req_line += f" ({config.METRICS['requests_retried']} с повтором)"
    tg_line = (
        f"Telegram: отправлено {config.METRICS['telegram_sent']}"
        + (f", не доставлено {config.METRICS['telegram_failed']}"
           if config.METRICS['telegram_failed'] else "")
    )
    # Опциональные строки — только при ненулевых счётчиках, чтобы сводка
    # прогона без push/LLM/огрызков не обрастала нулями.
    opt_lines: list[str] = []
    if config.METRICS["cards_degraded"]:
        opt_lines.append(f"Карточек-огрызков: {config.METRICS['cards_degraded']}")
    if config.METRICS["cards_captcha"]:
        opt_lines.append(
            f"Карточек под проверочным кодом: {config.METRICS['cards_captcha']}"
        )
    if config.METRICS["cards_blocked"]:
        opt_lines.append(
            f"Карточек не прочитано (заглушка/блок портала): "
            f"{config.METRICS['cards_blocked']}"
        )
    if config.METRICS["push_sent"] or config.METRICS["push_failed"]:
        opt_lines.append(
            f"Web Push: отправлено {config.METRICS['push_sent']}"
            + (f", сбоев {config.METRICS['push_failed']}"
               if config.METRICS["push_failed"] else "")
        )
    if (config.METRICS["llm_summary_calls"]
            or config.METRICS["llm_summary_cache_hits"]
            or config.METRICS["llm_summary_failed"]):
        opt_lines.append(
            f"LLM-пересказы актов: вызовов {config.METRICS['llm_summary_calls']}, "
            f"из кэша {config.METRICS['llm_summary_cache_hits']}"
            + (f", спасено фолбэком {config.METRICS['llm_summary_fallback_saved']}"
               if config.METRICS["llm_summary_fallback_saved"] else "")
            + (f", сбоев {config.METRICS['llm_summary_failed']} (откат на excerpt)"
               if config.METRICS["llm_summary_failed"] else "")
        )
    lines = [
        "=" * 60,
        f"Сводка прогона ({mode})",
        "=" * 60,
    ]
    if extras:
        # Превращаем extras в "k: v | k: v" в том порядке, в котором их передали
        lines.append(" | ".join(
            f"{_EXTRAS_RU.get(k, k)}: {v}" for k, v in extras.items()
        ))
    lines.append(req_line)
    lines.append(tg_line)
    lines.extend(opt_lines)
    if timings:
        lines.append(f"Тайминги: {_format_timings(timings)}")
    lines.append("=" * 60)

    for line in lines:
        log.info(line)

    # GitHub Actions: при наличии $GITHUB_STEP_SUMMARY дописываем markdown-блок,
    # который появится в UI раздела Summary у запуска workflow.
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        try:
            md_lines = [
                f"### Сводка прогона ({mode})",
                "",
            ]
            if extras:
                md_lines.append("| Метрика | Значение |")
                md_lines.append("| --- | --- |")
                for k, v in extras.items():
                    md_lines.append(f"| {_EXTRAS_RU.get(k, k)} | {v} |")
                md_lines.append("")
            md_lines.append(f"- {req_line}")
            md_lines.append(f"- {tg_line}")
            for opt in opt_lines:
                md_lines.append(f"- {opt}")
            if timings:
                md_lines.append(f"- Тайминги: `{_format_timings(timings)}`")
            md_lines.append("")
            with open(summary_path, "a", encoding="utf-8") as f:
                f.write("\n".join(md_lines))
        except Exception as e:
            log.warning(f"Не удалось записать GITHUB_STEP_SUMMARY: {e}")


# ── Аварийный алерт ──────────────────────────────────────────────────────────

def send_crash_alert(mode: str, exc: BaseException) -> None:
    """
    Попытаться сообщить в Telegram, что прогон упал.
    Не должен сам кидать исключение, иначе перекроет исходное.
    """
    try:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        tb_tail = tb[-1500:]  # хвост трейсбека, чтобы не упереться в лимит Telegram
        text = (
            "⚠️ <b>Прогон упал</b>\n"
            f"Режим: <code>{html_escape(mode)}</code>\n"
            f"Ошибка: <code>{html_escape(type(exc).__name__)}: {html_escape(str(exc))}</code>\n\n"
            f"<pre>{html_escape(tb_tail)}</pre>"
        )
        send_telegram(text)
    except Exception as alert_err:
        log.error(f"Не удалось отправить crash-алерт в Telegram: {alert_err}")


# ── Проверка окружения ───────────────────────────────────────────────────────
