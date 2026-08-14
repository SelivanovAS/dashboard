"""Пер-кейсовый отчёт парсинга трека «Иски банка» за один прогон.

Зачем: причины пропуска карточек в FI-цикле живут на DEBUG (боевой
LOG_LEVEL=INFO), а фейлы загрузки (капча/блок/HTTP) считаются только
глобальными METRICS без привязки к делу — из данных нельзя ответить
«почему это дело сегодня не проверялось». Аккумулятор собирает исход
каждого bank-дела и сохраняется в data/bank_parse_report.json (фаза 7c
main_json); файл коммитится workflow'ом и рендерится карточкой
«Парсинг исков банка» в админке Worker'а.

Ключ аккумулятора — идентичность dict дела (id(case)): промоушен М→2
меняет case["id"] посреди цикла, а объект остаётся тем же; номер/суд/
статус резолвятся при save() из финального состояния записи. Все методы
молча игнорируют дела не из трека (гейт is_bank_plaintiff_track) — точки
врезки в runs.py зовут их без собственных if.

Русские подписи причин считаются здесь и в skip_reason_ru — JS админки
логику не дублирует, а только группирует по outcome.
"""

from __future__ import annotations

from court_monitor import config
from court_monitor.config import log
from court_monitor.lifecycle import is_bank_plaintiff_track
from court_monitor.storage import bank_events_key, save_json

# Исходы «пошли парсить, но неудачно» (без бумпа last_checked_at).
FETCH_FAIL_OUTCOMES = ("fetch_captcha", "fetch_blocked", "fetch_http", "fetch_empty")
# Исходы «карточку не запрашивали — не по чему» (реестр/ссылка).
NO_CARD_OUTCOMES = ("court_disabled", "no_link", "bad_link")

# Подписи исходов, у которых нет машинной причины из should_skip_case.
_OUTCOME_RU = {
    "fetch_captcha": "суд показал проверочный код (капчу) вместо карточки",
    "fetch_blocked": "портал суда отдал заглушку/блокировку вместо карточки",
    "fetch_http": "HTTP-ошибка при загрузке карточки",
    "fetch_empty": "пустой ответ сервера суда",
    "court_breaker": "суд снят с обхода предохранителем — карточки не читаются "
                     "(заглушка/код/сеть), дело перечитается следующим прогоном",
    "empty_shell": "карточка пришла без таблиц («пустая шелуха») — проверка не засчитана",
    "court_disabled": "суд не из реестра активных судов 1-й инстанции",
    "no_link": "нет ссылки на карточку (ждём backfill_fi_links)",
    "bad_link": "ссылка на карточку не разобралась",
    "intake_new": "дело заведено авто-подхватом с выдачи суда в этом прогоне "
                  "(карточка прочитана при заведении)",
}

# Ключи METRICS, чья дельта вокруг единственного HTTP-запроса итерации
# атрибутирует фейл fetch'а к конкретному делу (см. metrics_snapshot /
# classify_fetch_failure).
_FETCH_METRIC_KEYS = (
    "cards_captcha", "cards_blocked", "requests_failed",
    "cards_breaker_skipped",
)


def metrics_snapshot() -> dict:
    """Снимок METRICS перед fetch_card_checked — вход classify_fetch_failure."""
    return {k: config.METRICS.get(k, 0) for k in _FETCH_METRIC_KEYS}


def classify_fetch_failure(before: dict) -> str:
    """Исход неудачной загрузки карточки по дельте METRICS.

    Работает только потому, что в итерации FI-цикла ровно один HTTP-запрос
    (один URL карточки после polite_delay) — дельта счётчиков однозначно
    принадлежит этому делу. Менять сигнатуру fetch_card_checked ради возврата
    причины нельзя — у него другие вызыватели (апелляция/кассация/тексты
    актов). Пустое тело ответа (HTTP 200 без контента) своей метрики не
    имеет — это остаточная ветка.
    """
    if config.METRICS.get("cards_captcha", 0) > before.get("cards_captcha", 0):
        return "fetch_captcha"
    if config.METRICS.get("cards_blocked", 0) > before.get("cards_blocked", 0):
        return "fetch_blocked"
    if config.METRICS.get("requests_failed", 0) > before.get("requests_failed", 0):
        return "fetch_http"
    # Предохранитель открылся между пре-чеком FI-цикла и самим fetch'ем
    # (например, на тексте акта того же суда) — гейт внутри fetch_card_checked
    # вернул "" без HTTP.
    if (config.METRICS.get("cards_breaker_skipped", 0)
            > before.get("cards_breaker_skipped", 0)):
        return "court_breaker"
    return "fetch_empty"


class BankParseReport:
    """Аккумулятор пер-кейсовых исходов обхода bank-дел в FI-цикле."""

    def __init__(self):
        # id(case) → строка отчёта; отдельно держим ссылку на сам dict —
        # финальные номер/суд/статус берём из него при save().
        self._rows: dict[int, dict] = {}
        self._cases: dict[int, dict] = {}

    def _row(self, case: dict) -> dict | None:
        key = id(case)
        # Сначала — существующая строка: split_bank_track (фаза 7c) снимает
        # маркер track у «переехавших» дел, и гейт по нему отрезал бы их же
        # пометки left_track/archived.
        existing = self._rows.get(key)
        if existing is not None:
            return existing
        if not is_bank_plaintiff_track(case):
            return None
        if key not in self._rows:
            self._rows[key] = {
                "outcome": "pending",
                "reason": "",
                "reason_ru": "",
                "detail": "",
                "degraded": False,
                "force_parsed": False,
                "events": [],
                "changed": False,
                "left_track": False,
                "archived": False,
            }
            self._cases[key] = case
        return self._rows[key]

    def seed(self, case: dict, in_queue: bool):
        """Завести строку до цикла. Не попавшие в очередь fi_active — сразу
        not_in_queue (стадия ушла выше — дело покидает трек в 7c, либо нет
        номера); сейчас этот класс дел не логируется вовсе."""
        row = self._row(case)
        if row is None or in_queue:
            return
        stage = case.get("current_stage") or "first_instance"
        row["outcome"] = "not_in_queue"
        row["detail"] = stage
        if stage != "first_instance":
            row["reason_ru"] = (
                f"стадия «{stage}» — карточку 1-й инст. не парсим, "
                f"дело уходит из лёгкого трека"
            )
        else:
            row["reason_ru"] = "нет номера дела 1-й инстанции"

    def record(self, case: dict, outcome: str,
               reason: str = "", reason_ru: str = "", detail: str = ""):
        """Основной исход итерации; поздний вызов перезаписывает ранний."""
        row = self._row(case)
        if row is None:
            return
        row["outcome"] = outcome
        row["reason"] = reason
        row["reason_ru"] = reason_ru or _OUTCOME_RU.get(outcome, "")
        row["detail"] = detail

    def mark_force_parsed(self, case: dict):
        row = self._row(case)
        if row is not None:
            row["force_parsed"] = True

    def mark_distrusted_date(self, case: dict, date_ru: str):
        """Дата заседания дальше горизонта доверия — похоже на опечатку суда.

        Отдельно от `force_parsed`: рутинная страховка и «суд написал 2029
        вместо 2026» — разные новости, а в отчёте они иначе неразличимы.
        """
        row = self._row(case)
        if row is not None:
            row["distrusted_date"] = date_ru

    def mark_degraded(self, case: dict):
        row = self._row(case)
        if row is not None:
            row["degraded"] = True

    def mark_events(self, case: dict, types: list, changed: bool):
        row = self._row(case)
        if row is not None:
            row["events"] = list(types or [])
            row["changed"] = bool(changed)

    def mark_left_track(self, case: dict):
        row = self._row(case)
        if row is not None:
            row["left_track"] = True

    def mark_track_moves(self):
        """После split_bank_track: пометить дела, покинувшие трек в этом
        прогоне — маркер track у них уже снят, след остался в track_origin.
        Дела, переехавшие в прошлые прогоны, в аккумулятор не попадают
        (на seed у них уже нет track)."""
        for key, case in self._cases.items():
            if (not is_bank_plaintiff_track(case)
                    and case.get("track_origin") == "plaintiff_light"):
                self._rows[key]["left_track"] = True

    def mark_archived(self, case: dict):
        row = self._row(case)
        if row is not None:
            row["archived"] = True

    def rows(self) -> list[dict]:
        """Строки отчёта с финальными реквизитами дел (после промоушенов)."""
        out = []
        for key, row in self._rows.items():
            case = self._cases[key]
            fi = case.get("first_instance") or {}
            resolved = dict(row)
            if resolved["outcome"] == "pending":
                # Дело было в очереди, но ни одна точка врезки не отметилась —
                # такого пути в цикле нет; страховка от будущих правок.
                resolved["outcome"] = "unknown"
            resolved.update({
                "key": bank_events_key(case),
                "number": (case.get("id") or fi.get("case_number") or "").strip(),
                "court": fi.get("court", ""),
                "court_domain": fi.get("court_domain", ""),
                "case_status": fi.get("status", ""),
                "last_checked_at": fi.get("last_checked_at", ""),
            })
            out.append(resolved)
        return out

    def totals(self, rows: list[dict] | None = None) -> dict:
        rows = self.rows() if rows is None else rows
        t = {"total": len(rows), "parsed": 0, "skip": 0, "failed": 0,
             "no_card": 0, "not_in_queue": 0, "intake_new": 0}
        for r in rows:
            o = r["outcome"]
            if o == "intake_new":
                # Заведённые в этом же прогоне карточку уже прочитали при
                # приёме — в «спарсено» их не считаем, чтобы X/Y сводки
                # оставался долей ОБХОДА существующего пула.
                t["intake_new"] += 1
            elif o == "parsed":
                t["parsed"] += 1
            elif o == "skip":
                t["skip"] += 1
            elif o in FETCH_FAIL_OUTCOMES or o in ("empty_shell", "court_breaker"):
                t["failed"] += 1
            elif o in NO_CARD_OUTCOMES:
                t["no_card"] += 1
            elif o == "not_in_queue":
                t["not_in_queue"] += 1
        return t

    def save(self, path: str, run_date, smart_skip: bool):
        """Записать отчёт (атомарно, через save_json — тот ставит updated_at)."""
        rows = self.rows()
        save_json({
            "version": 1,
            "run_date": run_date.isoformat(),
            "smart_skip": bool(smart_skip),
            "totals": self.totals(rows),
            "cases": rows,
        }, path)


def save_bank_parse_report(report: BankParseReport, run_date, smart_skip: bool):
    """Обёртка фазы 7c: отчёт не имеет права ронять прогон (образец —
    🩺-блоки main_json), поэтому любые ошибки записи гасятся в WARNING."""
    try:
        report.save(config.BANK_PARSE_REPORT_PATH, run_date, smart_skip)
    except Exception as e:  # noqa: BLE001 — сервисный канал, не пайплайн
        log.warning(f"Отчёт парсинга исков банка не записан: {e}")
