# -*- coding: utf-8 -*-
"""Конфигурация: env-переменные, пути данных, окна state-machine,
логгер и метрики прогона.

Все значения читаются из окружения ОДИН раз при импорте (семантика
монолита сохранена). Патчабельные константы другие модули читают только
атрибутным доступом `config.X` — так monkeypatch.setattr(config, ...)
в тестах действует на все места чтения.
"""

from __future__ import annotations

import logging
import os

CSV_PATH = os.environ.get("CSV_PATH", "data/sberbank_cases.csv")
CSV_ARCHIVE_PATH = os.environ.get(
    "CSV_ARCHIVE_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "sberbank_cases_archive.csv")
)
JSON_PATH = os.environ.get("JSON_PATH", "data/cases.json")
JSON_ARCHIVE_PATH = os.environ.get(
    "JSON_ARCHIVE_PATH",
    os.path.join(os.path.dirname(JSON_PATH) or "data", "cases_archive.json")
)


def cold_archive_path(year: int) -> str:
    """Путь к «холодному» годовому архиву cases_archive_YYYY.json (лежит рядом
    с горячим JSON_ARCHIVE_PATH). Фронт эти файлы не грузит — см.
    rotate_cold_archive."""
    base = os.path.dirname(JSON_ARCHIVE_PATH) or "data"
    return os.path.join(base, f"cases_archive_{year}.json")


def cold_archive_glob() -> str:
    """Glob-шаблон всех холодных годовых архивов — для подмешивания их id
    в индекс дедупликации (см. main_json)."""
    base = os.path.dirname(JSON_ARCHIVE_PATH) or "data"
    return os.path.join(base, "cases_archive_*.json")
DIGESTED_ACTS_PATH = os.environ.get(
    "DIGESTED_ACTS_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", ".digested_acts")
)
# Дедуп кассационных определений: ключи «8Г-номер|дата акта», чьи new_act
# уже уходили в дайджест. Без него «мигание» act_published (сбойный парс
# карточки 7kas перезаписывает блок с False, следующий удачный снова ставит
# True) даёт повторный new_act → дубль пересказа определения в дайджесте.
CASSATION_ACTS_PATH = os.environ.get(
    "CASSATION_ACTS_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", ".cassation_acts")
)
# Кэш LLM-пересказов мотивировок: {sha1(act_text)[:16]: {summary, model,
# stage, generated_at}}. Хранится отдельно от .digested_acts (тот — set
# номеров дел, а здесь — мапа hash→текст). Кэш переживает --replay-last
# и повторные прогоны: один и тот же act_text не пересказываем дважды.
ACT_SUMMARIES_PATH = os.environ.get(
    "ACT_SUMMARIES_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", ".act_summaries.json")
)
# Снимок контекста последнего дайджеста — сохраняется перед отправкой
# в Telegram и используется режимом --replay-last для повторной генерации
# (например, чтобы переиграть с другой версией промпта).
LAST_DIGEST_CONTEXT_PATH = os.environ.get(
    "LAST_DIGEST_CONTEXT_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "last_digest_context.json")
)
# Готовый текст последнего дайджеста (HTML) — сохраняется после успешной
# отправки в Telegram, фронт читает этот файл и показывает свёрнутый блок
# «Последний дайджест» в дашборде.
LAST_DIGEST_PATH = os.environ.get(
    "LAST_DIGEST_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "last_digest.json")
)
# Журнал последней push-рассылки: какие payload'ы ушли каждой подписке.
# Используется админкой подписчиков для отладки персональной фильтрации
# (видеть, какой именно вариант — personal/general/skip — получила каждая).
LAST_PERSONAL_PUSHES_PATH = os.environ.get(
    "LAST_PERSONAL_PUSHES_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "last_personal_pushes.json")
)
# Журнал здоровья парсеров: пер-источник история количества результатов
# поиска (суды 1-й инст., апелляция, 7kas). Детектор «молчаливой поломки»:
# суд, стабильно дававший результаты, вдруг отдаёт 0 (смена вёрстки,
# слетевший матчер судов) — без истории это неотличимо от «нет новостей».
# См. update_parse_health и блок 4e в main_json.
PARSE_HEALTH_PATH = os.environ.get(
    "PARSE_HEALTH_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "parse_health.json")
)
PARSE_HEALTH_HISTORY_LEN = 14   # сколько последних успешных прогонов помним
PARSE_HEALTH_FAIL_ALERT = 3     # HTTP-фейлов подряд до алерта
PARSE_HEALTH_DEGRADED_ALERT = 5  # карточек-«огрызков» за прогон до алерта
# Окна жизненного цикла дела (state machine — см. advance_case_stage /
# is_case_archived). Старая модель ARCHIVE_DAYS/ARCHIVE_DAYS_FI отсчитывала
# архивацию от даты последнего события — ненадёжный якорь, не учитывал ни
# кассационный срок (3 мес), ни задержку мотивировки. Новые окна привязаны
# к стадиям процесса и датам заседаний.
FI_ARCHIVE_DAYS = 60            # 1-я инстанция: 60 дней от даты резолютивки
                                # без подачи апел. жалобы → архив. Раньше было
                                # 45, но мотивировка часто задерживается на
                                # 2-3 недели, плюс 1 мес. на жалобу по ст. 321
                                # ГПК + лаг парсера на обновление карточки —
                                # реальное окно «решение → запись о жалобе» до
                                # 60-70 дней. Архив теперь не финален: при
                                # появлении жалобы дело возвращается в активные
                                # через reactivate_archived_first_instance.
APPEAL_NO_ACT_GRACE_DAYS = 30   # Апелляция: если акт не опубликован через
                                # 30 дней от апел. заседания — всё равно
                                # переходим в cassation_watch.
CASSATION_WATCH_DAYS = 120      # cassation_watch: 4 мес (≈3 мес срок + почта
                                # + регистрация) от апел. заседания. После —
                                # архив, если касс. жалоба так и не подана.
# Кассация (стадия cassation, парсер 7kas.sudrf.ru):
CASSATION_ACT_ARCHIVE_DAYS = 30      # 30 дней после публикации опред. → архив.
CASSATION_NO_ACT_PUBLISH_DAYS = 45   # 45 дней от даты вынесения опред. без
                                     # публикации текста → архив без акта.
# Ротация архива: дела, заархивированные более года назад (по archived_at),
# уезжают из «горячего» cases_archive.json (его грузит фронт) в «холодные»
# годовые файлы cases_archive_YYYY.json, которые фронт не загружает. Так вес
# того, что качает браузер, перестаёт расти безгранично. См. rotate_cold_archive.
COLD_ARCHIVE_DAYS = 365
# Legacy: CSV-ветка архивации (apelljatsiя в CSV) ещё использует старое
# 30-дневное окно от «Даты события». Будет удалена вместе с CSV-веткой.
LEGACY_CSV_ARCHIVE_DAYS = 30
REQUEST_DELAY = (2, 3)  # Задержка между запросами к суду (сек)
FETCH_MAX_RETRIES = 3   # Кол-во попыток загрузки страницы
DASHBOARD_URL = "https://selivanovas.github.io/dashboard/sberbank_dashboard.html"

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Web Push (PWA-уведомления)
PUSH_WORKER_URL = os.environ.get("PUSH_WORKER_URL", "").rstrip("/")
PUSH_SECRET = os.environ.get("PUSH_SECRET", "")
# Приватный VAPID-ключ в PEM-формате; хранится только в GitHub Secrets.
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "")

# Переключатель провайдера LLM: "claude" (по умолчанию) или "gigachat".
# Задаётся в workflow digest_only_gigachat.yml для отдельного прогона
# дайджеста через GigaChat. Основной мониторинг (update_cases.yml) остаётся
# на Claude и ничего не знает про этот флаг.
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "claude").strip().lower()

# Откат к старой архитектуре дайджеста (полный LLM-вызов с большим контекстом).
# По умолчанию используется гибридный путь: программный рендер
# (generate_template_digest) + LLM-микро-вызов только на пересказ
# мотивировок судебных актов (summarize_act_motivation). Флаг
# `DIGEST_FULL_LLM=1` возвращает старое поведение: ровно тот HTML,
# который выдавал Claude/GigaChat одним вызовом. Используется как
# escape hatch на случай регресса стилистики или необходимости A/B.
DIGEST_FULL_LLM = (
    os.environ.get("DIGEST_FULL_LLM", "").strip().lower() in ("1", "true", "yes")
)

# Включение LLM-полировщика готового HTML (вариант C1 итерации 2).
# Программа собирает черновик через generate_template_digest + пересказы
# актов; при `DIGEST_POLISH=1` черновик уходит в polish_digest_html, где
# LLM делает косметические правки (капитализация, жирные даты, склонения,
# сокращение длинных категорий). Валидатор проверяет контракт <a><b>NUM</b></a>;
# при провале — откат к черновику. По умолчанию выключен — для безопасности.
DIGEST_POLISH = (
    os.environ.get("DIGEST_POLISH", "").strip().lower() in ("1", "true", "yes")
)

# Программный линтер готового дайджеста (digest/lint.py): детерминированные
# проверки HTML после отправки (полнота номеров, счётчики (N), баланс тегов,
# футер, лимит). Дайджест НЕ блокирует — при аномалии уходит сервисный
# 🩺-алерт в Telegram (по образцу детектора здоровья парсеров). Включён по
# умолчанию; DIGEST_LINT=0 — аварийный выключатель.
DIGEST_LINT = (
    os.environ.get("DIGEST_LINT", "").strip().lower()
    not in ("0", "false", "no")
)
GIGACHAT_AUTH_KEY = os.environ.get("GIGACHAT_AUTH_KEY", "")
GIGACHAT_SCOPE = os.environ.get("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")
GIGACHAT_MODEL = os.environ.get("GIGACHAT_MODEL", "GigaChat")
GIGACHAT_OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
GIGACHAT_API_URL = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"

# Лимит Telegram на одно сообщение
TELEGRAM_MSG_LIMIT = 4096
# Целевой лимит длины дайджеста (передаётся в промпт). Должен быть ЗАМЕТНО
# больше реального объёма — иначе Haiku 4.5 в режиме «экономии» сворачивает
# дела в одну строку и выкидывает события, чтобы уложиться. Готовый HTML
# дайджеста БОЛЬШЕ не обрезаем (было truncate_html_message(text, 2×4096)):
# дашборд рендерит его целиком, а send_telegram через split_message сам
# разбивает на сообщения по 4096 без потери содержимого — фактического
# лимита на объём нет, зажимать LLM смысла нет.
DIGEST_CHAR_LIMIT = 12000

# Окно свежести для событий-жалоб в дайджесте: «подана апел./касс. жалоба» и
# «направлено в касс. суд» с датой старше N дней в дайджест не идут (флаги и
# переходы стадий не затрагиваются). Ловит первый парс старых карточек
# (backfill/discovery): жалоба октября-2025, впервые увиденная в июле-2026, —
# не новость. Анонсы заседаний фильтруются жёстче — по «дата в прошлом».
DIGEST_STALE_EVENT_DAYS = 45

# Паттерны для опознания «Сбербанка» среди сторон дела (lowercase substring match).
# Используется и при первичном парсинге поисковой выдачи, и при определении
# апеллянта на стадии обновления карточки. Должен быть один источник истины,
# иначе роль банка проставляется неконсистентно.
SBER_PATTERNS = ("сбербанк", "сбербанк россии", "пао сбербанк", "пао сбер")

CSV_COLUMNS = [
    "Номер дела", "Дата поступления", "Истец", "Ответчик", "Категория",
    "Суд 1 инстанции", "Судья 1 инстанции", "Роль банка", "Статус",
    "Последнее событие", "Дата события", "Время заседания",
    "Акт опубликован", "Результат", "Ссылка", "Заметки", "Апеллянт",
    "Дата публикации акта", "Дата заседания", "Судья-докладчик"
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("court-monitor")
# ── Метрики прогона ──────────────────────────────────────────────────────────

# Глобальные счётчики прогона — собираются по ходу выполнения,
# сбрасываются в начале каждого main()/main_digest_only().
METRICS: dict[str, int] = {
    "requests_ok": 0,
    "requests_failed": 0,
    "requests_retried": 0,   # попытки fetch_page после неудачи
    "telegram_sent": 0,      # успешно отправленных сообщений (после split)
    "telegram_failed": 0,    # полностью не отправленных частей
    "cards_degraded": 0,     # карточек-«огрызков» без событий за прогон
}


def _metrics_reset() -> None:
    for k in METRICS:
        METRICS[k] = 0
