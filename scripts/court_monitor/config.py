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
import re
import sys

from court_monitor import ghlog

# Активный регион мониторинга (реестры судов — scripts/court_monitor/regions/).
# Приоритет: env REGION (workflows, pytest-conftest) → файл REGION в корне
# репо (способ форка территории: файл коммитится в форк, merge=ours — регион
# не потеряется, даже если забыли Actions Variable) → дефолт hmao (эталон).
# Резолвится в RegionConfig через regions.get_region() — тот читает
# config.REGION на каждый вызов (тесты патчат monkeypatch.setattr(config, ...)).
_REGION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "REGION")


def _region_from_file() -> str:
    try:
        with open(_REGION_FILE, encoding="utf-8") as f:
            return f.read().strip().lower()
    except OSError:
        return ""


REGION = (os.environ.get("REGION", "") or _region_from_file() or "hmao").strip().lower()

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
# Трек «Иски банка» (банк — истец): отдельный файл, чтобы основной cases.json
# не рос (фронт грузит его лениво, Worker не парсит вовсе). Схема записей —
# та же, что в cases.json, плюс маркер track="plaintiff_light".
JSON_BANK_PATH = os.environ.get(
    "JSON_BANK_PATH",
    os.path.join(os.path.dirname(JSON_PATH) or "data", "cases_bank.json")
)
JSON_BANK_ARCHIVE_PATH = os.environ.get(
    "JSON_BANK_ARCHIVE_PATH",
    os.path.join(os.path.dirname(JSON_PATH) or "data", "cases_bank_archive.json")
)
# Хроника (events) bank-дел хранится отдельно от списка: events — ~64% веса
# записи, а фронту они нужны только в drawer. Список (cases_bank.json) фронт
# грузит при входе в картотеку банка, events-файл — лениво при первом
# открытии карточки. Ключ мапы — «домен|номер» (номера дел не уникальны
# между судами). Содержимое events не меняется — только место хранения.
JSON_BANK_EVENTS_PATH = os.environ.get(
    "JSON_BANK_EVENTS_PATH",
    os.path.join(os.path.dirname(JSON_BANK_PATH) or "data", "cases_bank_events.json")
)
JSON_BANK_ARCHIVE_EVENTS_PATH = os.environ.get(
    "JSON_BANK_ARCHIVE_EVENTS_PATH",
    os.path.join(os.path.dirname(JSON_BANK_ARCHIVE_PATH) or "data",
                 "cases_bank_archive_events.json")
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


def bank_cold_archive_path(year: int) -> str:
    """Холодный годовой архив трека «Иски банка» cases_bank_archive_YYYY.json.
    В отличие от горячих bank-файлов, холодные хранят ПОЛНЫЕ записи с inline
    events (write-only: фронт их не грузит, прогон читает только в дедуп)."""
    base = os.path.dirname(JSON_BANK_ARCHIVE_PATH) or "data"
    return os.path.join(base, f"cases_bank_archive_{year}.json")


def bank_cold_archive_glob() -> str:
    """Glob холодных bank-архивов. ⚠️ Шаблон «cases_bank_archive_*.json»
    матчит и events-файл горячего архива — потребители обязаны фильтровать
    имена по годовому суффиксу через is_bank_cold_archive_file()."""
    base = os.path.dirname(JSON_BANK_ARCHIVE_PATH) or "data"
    return os.path.join(base, "cases_bank_archive_*.json")


def is_bank_cold_archive_file(path: str) -> bool:
    """True для cases_bank_archive_YYYY.json (и только для них): отсекает
    cases_bank_archive_events.json, который тоже попадает под glob."""
    name = os.path.basename(path)
    return bool(re.fullmatch(r"cases_bank_archive_\d{4}\.json", name))
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
# Пер-кейсовый отчёт парсинга трека «Иски банка» за последний прогон: какие
# дела парсились, какие пропущены и почему (пишет BankParseReport в фазе 7c
# main_json, читает карточка «Парсинг исков банка» в админке Worker'а).
BANK_PARSE_REPORT_PATH = os.environ.get(
    "BANK_PARSE_REPORT_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "bank_parse_report.json")
)
# Негативный кэш авто-подхвата исков банка: строки выдачи, которые правила
# приёма отвергли по КАРТОЧКЕ (итог из списка исключений, уже выданный ИЛ) —
# в дедуп-индекс они не попадают, и без памяти прогон качал бы их карточки
# каждый день, пока строка не уедет с первой страницы выдачи.
BANK_INTAKE_SEEN_PATH = os.environ.get(
    "BANK_INTAKE_SEEN_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", ".bank_intake_seen.json")
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
# Трек «Иски банка» (банк — истец, data/cases_bank.json): свои окна — обычное
# FI_ARCHIVE_DAYS=60 от резолютивки убивало бы ожидание исполнительного листа
# (мотивировка ≤10 раб. дн → месяц на апелляцию → выдача ИЛ: реальный лист
# появляется на +40..90+ день). См. is_case_archived / should_skip_case.
BANK_TRACK = os.environ.get("BANK_TRACK", "1") == "1"  # мастер-выключатель трека
BANK_WRIT_CHECK_DAYS = 7        # решённый иск банка: опрос карточки раз в 7 дн
                                # (ловим апел. жалобу до вступления в силу и ИЛ
                                # после; ежедневный парс бесполезен — события
                                # там штучные).
# Слагаемые расчётной даты вступления решения в силу (bank_legal_force_est).
# Сроки в днях по ГПК — РАБОЧИЕ дни (ст. 107: нерабочие не включаются);
# месячный срок апелляции (ст. 321) константы не требует — он календарный
# (ст. 108, month_term_last_day).
BANK_MOTIVATION_TERM_WORKDAYS = 10   # ст. 199 ГПК: изготовление мотивированного
                                     # решения — расчётный фолбэк, когда нет ни
                                     # act_date, ни события «Изготовлено
                                     # мотивированное решение».
BANK_DEFAULT_COPY_SEND_WORKDAYS = 3  # ст. 236 ГПК: направление копии заочного
                                     # решения ответчику (формула ВС, Обзор №2
                                     # (2015), вопрос 14).
BANK_DEFAULT_CANCEL_WORKDAYS = 7     # ст. 237 ГПК: заявление об отмене заочного
                                     # решения (от вручения копии либо в составе
                                     # формулы ВС).
BANK_WRIT_WAIT_MAX_DAYS = 180   # потолок ожидания ИЛ от расчётного вступления
                                # в силу: лист так и не появился → архив (иначе
                                # пул опрашивался бы вечно).
BANK_WRIT_ARCHIVE_DAYS = 14     # ИЛ выдан → архив через 14 дн (окно на смену
                                # статуса листа: «Отозван»/«Возвращен»).
BANK_DEFAULT_WRIT_ARCHIVE_DAYS = 90  # ЗАОЧНОЕ решение + ИЛ выдан → архив через
                                     # 3 мес. вместо 14 дн (решение юриста
                                     # 03.08.2026). Ответчик подаёт заявление об
                                     # отмене в тот же суд (ст. 237 ГПК), и суды
                                     # реально отменяют заочные решения спустя
                                     # 1-2 месяца — по 14-дневному окну дело
                                     # исчезало бы из картотеки раньше, чем
                                     # станет ясно, устояло ли решение (три
                                     # дела Сургутского гор. ушли так 27.07).
BANK_DEFAULT_CANCEL_PENDING_MAX_DAYS = 90  # потолок ожидания определения по
                                     # заявлению об отмене: суд обязан
                                     # рассмотреть за 10 дн (ст. 240), но
                                     # результат в карточке может не появиться
                                     # никогда — без потолка дело висело бы в
                                     # активных вечно (ветка pending снимает и
                                     # архивацию, и недельный ритм опроса).
BANK_RETURNED_ARCHIVE_DAYS = 30  # иск возвращён/прекращён → архив через 30 дн
                                 # (окно на частную жалобу банка).
BANK_MERGED_ARCHIVE_DAYS = 30    # дело присоединено к другому (ст. 151 ГПК) →
                                 # архив через 30 дн от определения (окно на
                                 # отмену объединения). Статус карточки при этом
                                 # остаётся «В производстве» — без своей ветки
                                 # дело висело бы активным вечно.
BANK_DENIED_ARCHIVE_DAYS = 30    # в иске ОТКАЗАНО → листа не будет, ждём только
                                 # апел. жалобу банка: архив через 30 дн от
                                 # мотивировки (≈ срок обжалования, ст. 321 ГПК).
# Рутина track-дел в дайджесте (заседания, смены статусов, принятия): пилот
# шлёт всё (решение юриста 25.07.2026); при масштабировании на ~1000 дел
# рутина затопит Telegram-лимит (~7600 симв.) — выключается env=0, остаются
# решение/итог, возврат, апел. жалоба и ИЛ. См. filter_bank_routine_events.
BANK_DIGEST_ROUTINE = os.environ.get("BANK_DIGEST_ROUTINE", "1") == "1"
# Авто-подхват исков банка ежедневным прогоном (блок 3b фазы 3): страница
# выдачи и так качается для поиска ответчик-дел, истцовые строки с неё уходят
# в лёгкий трек. До 31.07.2026 трек пополнялся только вручную (реестр или
# разовый сборщик), т.е. новый иск вставал на мониторинг лишь после того, как
# юрист вспомнит запустить сбор, — и ранние исполнительные листы терялись.
BANK_AUTO_INTAKE = os.environ.get("BANK_AUTO_INTAKE", "1") == "1"
# Холостой прогон авто-подхвата: кандидаты считаются и логируются по строковым
# фильтрам, карточки НЕ качаются, записи не создаются. Для первого боевого
# прогона после деплоя — посмотреть реальные цифры без риска.
BANK_INTAKE_DRY_RUN = os.environ.get("BANK_INTAKE_DRY_RUN", "0") == "1"
BANK_INTAKE_MAX_PER_RUN = int(os.environ.get("BANK_INTAKE_MAX_PER_RUN", "30") or 30)
# Потолок карточек-кандидатов на ОДИН суд. Фаза 3 идёт раньше FI-цикла, и
# пять подряд нечитаемых карточек открывают пер-судовый предохранитель
# (CARD_BREAKER_THRESHOLD) — без кэпа подхват мог бы в одиночку снять суд с
# обхода карточек на весь прогон.
BANK_INTAKE_MAX_CARDS_PER_COURT = int(
    os.environ.get("BANK_INTAKE_MAX_CARDS_PER_COURT", "10") or 10
)
BANK_INTAKE_ALERT_ADDED = 50    # заведено больше за прогон → 🩺-алерт (паводок:
                                # молча сломался дедуп или суд опубликовал архив)
BANK_INTAKE_SEEN_TTL_DAYS = 60  # сколько помним отказников негативного кэша
# Legacy: CSV-ветка архивации (apelljatsiя в CSV) ещё использует старое
# 30-дневное окно от «Даты события». Будет удалена вместе с CSV-веткой.
LEGACY_CSV_ARCHIVE_DAYS = 30
REQUEST_DELAY = (2, 3)  # Задержка между запросами к суду (сек)
# Кол-во попыток загрузки страницы. Боевой прогон — ОДНА попытка (решение
# юриста 26.07.2026): пропуск безопасен — карточка перечитается следующим
# прогоном, сбой поиска пишется в журнал здоровья как HTTP-fail (алерт только
# после 3 сбойных прогонов подряд), а ретраи при массовом сбое sudrf
# растягивали страницу до 105 с (3×30 с таймаута + паузы 5/10 с). Ручным
# пробам/импортам их workflow возвращают 3 через env — там запросов мало,
# а повтор запуска — ручной труд оператора.
FETCH_MAX_RETRIES = int(os.environ.get("FETCH_MAX_RETRIES", "1"))
# Пер-суд предохранитель карточек (аутейдж Сургутского 29.07.2026: заглушка
# на каждой карточке — прогон впустую молотил polite_delay + HTTP по всем
# делам суда). Столько НЕ прочитанных карточек ПОДРЯД (заглушка, проверочный
# код, сетевой фейл — вперемешку) отключают суд до конца прогона: дальнейшие
# карточки этого хоста пропускаются без HTTP (netutil.card_breaker_allows).
# Успешная карточка сбрасывает счётчик. 0 — предохранитель выключен (ручные
# пробы лежащего суда). Память — ОДИН прогон (сброс в _metrics_reset).
CARD_BREAKER_THRESHOLD = int(os.environ.get("CARD_BREAKER_THRESHOLD", "5"))
# Half-open: у отключённого суда каждая K-я пропущенная карточка идёт как
# проба — суд ожил в середине прогона → обход возобновляется (короткая
# заглушка не отнимает суд на сутки). Заодно самостраховка канарейки
# (пре-открытие по заглушке на странице ПОИСКА при живых карточках
# самоисправится первой же пробой). 0 — без проб.
CARD_BREAKER_PROBE_EVERY = int(os.environ.get("CARD_BREAKER_PROBE_EVERY", "25"))
# Пер-кейсовый smart-skip (should_skip_case): пропуск карточек с известной
# будущей датой (заседание / «без движения»). Выставляется в main_json из
# флага --smart-skip / env SKIP_NON_WORKING_DAYS: крон передаёт его всегда,
# ручной запуск без галки — полный прогон всех активных карточек.
# Дефолт True — для прочих режимов (CSV-ветка, тесты) поведение прежнее.
SMART_SKIP_CASES = True
# URL дашборда территории: env DASHBOARD_URL (Actions Variable форка) →
# дефолт региона (regions/<code>.py). Ленивый импорт не нужен: regions/__init__
# на уровне модуля тянет только stdlib+base, цикла с config нет (get_region
# читает config уже из функции).
from court_monitor.regions import get_region as _get_region  # noqa: E402
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "") or _get_region().dashboard_url

# Контакт в VAPID-claims Web Push (sub) — свой у каждой территории.
VAPID_SUB_EMAIL = os.environ.get("VAPID_SUB_EMAIL", "mailto:7selivanov.a@gmail.com")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
# Личный чат юриста. Нужен, чтобы отличить «дайджест идёт мне» от «дайджест
# идёт в корпоративную группу»: сервисная приписка о LLM-модели добавляется
# к Telegram-версии дайджеста только при совпадении TELEGRAM_CHAT_ID с этим
# значением (см. _telegram_digest_text в runs.py). Workflow'ы передают сюда
# secrets.TELEGRAM_CHAT_ID_TEST; если переменная не задана — приписки нет.
TELEGRAM_CHAT_ID_PERSONAL = os.environ.get("TELEGRAM_CHAT_ID_PERSONAL", "")

# Web Push (PWA-уведомления)
PUSH_WORKER_URL = os.environ.get("PUSH_WORKER_URL", "").rstrip("/")
PUSH_SECRET = os.environ.get("PUSH_SECRET", "")
# Приватный VAPID-ключ в PEM-формате; хранится только в GitHub Secrets.
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "")

# Переключатель провайдера LLM: "claude" (по умолчанию), "gigachat"
# или "openrouter". Тестовый workflow (test_digest.yml) пробрасывает выбор
# провайдера/модели из inputs; основной мониторинг (update_cases.yml)
# остаётся на Claude и ничего не знает про этот флаг.
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "claude").strip().lower()

# Модель Claude для дайджеста. По умолчанию — боевой эталон haiku (общий кэш
# пересказов). Тестовый workflow (test_digest.yml) может выбрать sonnet/opus
# через input claude_model → env CLAUDE_MODEL; короткие имена из админки
# резолвятся в полный id API, точный id (из «Точной модели») проходит как есть.
# Основной мониторинг (update_cases.yml) env CLAUDE_MODEL не ставит → остаётся
# на haiku. Код читает только config.CLAUDE_MODEL — тесты патчат его напрямую.
DEFAULT_CLAUDE_MODEL = "claude-haiku-4-5-20251001"
_CLAUDE_MODEL_ALIASES = {
    "haiku": DEFAULT_CLAUDE_MODEL,
    "sonnet": "claude-sonnet-5",
    "opus": "claude-opus-4-8",
}


def resolve_claude_model(raw: str) -> str:
    """Короткое имя (haiku/sonnet/opus) → полный id модели Claude.

    Пусто → эталон haiku. Неизвестное значение (точный id из «Точной модели»)
    проходит как есть. Регистр короткого имени не важен.
    """
    val = (raw or "").strip()
    return _CLAUDE_MODEL_ALIASES.get(val.lower(), val) or DEFAULT_CLAUDE_MODEL


CLAUDE_MODEL = resolve_claude_model(os.environ.get("CLAUDE_MODEL", ""))

# Уровень усилий (output_config.effort в API) для моделей Claude нового
# поколения (Opus 4.7+/Sonnet 5): управляет глубиной adaptive-размышлений и
# расходом токенов. Пусто = параметр не отправляется (дефолт API — high).
# Haiku эффорт не поддерживает (API вернёт ошибку) — для неё значение
# игнорируется на уровне сборки пейлоада (llm._claude_payload).
_CLAUDE_EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")


def resolve_claude_effort(raw: str) -> str:
    """Нормализовать уровень усилий: неизвестное значение (в т.ч. «default»
    из селектора админки) → пусто, т.е. дефолт API. Регистр не важен."""
    val = (raw or "").strip().lower()
    return val if val in _CLAUDE_EFFORT_LEVELS else ""


CLAUDE_EFFORT = resolve_claude_effort(os.environ.get("CLAUDE_EFFORT", ""))

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
# `or "GigaChat"` — workflow передаёт общий input llm_model и в GIGACHAT_MODEL,
# и в OPENROUTER_MODEL; пустая строка из env должна означать «дефолт модели».
GIGACHAT_MODEL = os.environ.get("GIGACHAT_MODEL", "").strip() or "GigaChat"
GIGACHAT_OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
GIGACHAT_API_URL = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
# Модели 3-го поколения (GigaChat-3-Ultra, freemium для физлиц) живут на
# отдельном базовом адресе — стандартный gigachat.devices.sberbank.ru их
# не принимает. Выбор URL по модели — llm._gigachat_api_url.
GIGACHAT_V3_API_URL = "https://api.giga.chat/v1/chat/completions"

# OpenRouter — третий провайдер (только тестовый контур, см. test_digest.yml).
# OPENROUTER_MODEL: буквальный id модели ИЛИ место в рейтинге бесплатных
# моделей («модель дня (топ-1)», «топ-3» — значения выпадающего списка
# workflow; пусто = топ-1). Место резолвится на прогоне
# (llm._resolve_openrouter_model) из OPENROUTER_TOP_MODELS_URL (рейтинг
# shir-man.com/free-llm), при недоступности — OPENROUTER_FALLBACK_MODEL
# (маршрут openrouter/free: OpenRouter сам выбирает живую бесплатную модель).
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "").strip()
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_TOP_MODELS_URL = "https://shir-man.com/api/free-llm/top-models"
OPENROUTER_FALLBACK_MODEL = "openrouter/free"

# Ретраи LLM-пересказов актов через OpenRouter: перегруженный free-пул отдаёт
# 429 мгновенно, и немедленный повтор упирается в ту же стену — между
# попытками нужна пауза (нарастающая, attempt * DELAY: 5с, 10с — как у
# fetch_page). Если основная модель так и не ответила — фолбэк-роутер
# OPENROUTER_FALLBACK_MODEL (openrouter/free: OpenRouter сам подбирает живую
# бесплатную модель), тоже с ретраем. Худший случай на безнадёжный акт:
# 3+2 вызова и ~20 с пауз. Инцидент 17.07.2026 (Урал): два хвостовых акта
# из шести ушли в дайджест сырой мотивировкой вместо «Почему:».
OPENROUTER_SUMMARY_RETRIES = 3           # попыток на основной модели
OPENROUTER_SUMMARY_FALLBACK_RETRIES = 2  # попыток на фолбэк-модели
OPENROUTER_SUMMARY_RETRY_DELAY = 5       # база паузы между попытками (сек)

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

# Паттерны дочерних структур Сбербанка, которые НЕ являются ПАО Сбербанк
# (страхование, НПФ, УК и т.п.). Живут в config, а не в parsing/search:
# lifecycle/linking не могут импортировать parsing (цикл), а фильтр дочек
# нужен и им — с 09.08.2026 податель жалобы «банк» только когда это сам
# ПАО Сбербанк (кейс 8Г-11469/2026: 🏦 вставал на жалобу ООО «Сбербанк
# страхование жизни»). Порядок не важен — применяются последовательно.
SBER_SUBSIDIARY_PATTERNS = [
    # Сбербанк страхование [жизни] — СК ООО/АО «Сбербанк страхование жизни» и варианты
    re.compile(r'сбербанк\s+страхован\w*(?:\s+жизн\w*)?', re.IGNORECASE),
    # НПФ Сбербанк — АО «НПФ Сбербанк», «Негосударственный пенсионный фонд Сбербанк»
    re.compile(r'нпф\s+сбербанк', re.IGNORECASE),
    re.compile(r'негосударственн\w*\s+пенсионн\w*\s+фонд\w*\s+сбербанк', re.IGNORECASE),
    # Сбербанк Управление Активами — УК
    re.compile(r'сбербанк\s+управлен\w*\s+актив\w*', re.IGNORECASE),
    # Сбербанк Лизинг
    re.compile(r'сбербанк\s+лизинг\w*', re.IGNORECASE),
    # Сбербанк Факторинг
    re.compile(r'сбербанк\s+факторинг\w*', re.IGNORECASE),
]


def name_is_real_sberbank(name: str) -> bool:
    """True, если имя содержит сам ПАО Сбербанк, а не только дочку.

    Сначала вырезаются названия дочерних структур, затем остаток матчится
    по SBER_PATTERNS — «пао сбер» и прочие короткие формы продолжают
    работать. Пустое имя и имя без упоминания Сбера → False.
    """
    nm = (name or "").lower()
    for pat in SBER_SUBSIDIARY_PATTERNS:
        nm = pat.sub("", nm)
    return any(p in nm for p in SBER_PATTERNS)

CSV_COLUMNS = [
    "Номер дела", "Дата поступления", "Истец", "Ответчик", "Категория",
    "Суд 1 инстанции", "Судья 1 инстанции", "Роль банка", "Статус",
    "Последнее событие", "Дата события", "Время заседания",
    "Акт опубликован", "Результат", "Ссылка", "Заметки", "Апеллянт",
    "Дата публикации акта", "Дата заседания", "Судья-докладчик"
]

# Уровень логирования: LOG_LEVEL=DEBUG включает диагностику (skip-строки
# по каждому делу, полные списки не-HMAO судов, нераспарсенные даты и т.п.).
# Неизвестное значение молча откатывается на INFO — прогон важнее строгости.
_LOG_LEVEL_RAW = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
if _LOG_LEVEL_RAW not in ("DEBUG", "INFO", "WARNING", "ERROR"):
    _LOG_LEVEL_RAW = "INFO"
# stdout, а не дефолтный stderr: workflow-команды GitHub (::group::,
# ::warning:: из ghlog) читаются из stdout — два потока перепутали бы
# порядок строк. Mac-обёртка пишет `>>"$LOG" 2>&1` — ей без разницы,
# а StreamHandler флашит каждую запись, так что буферизация не страшна.
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL_RAW),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("court-monitor")
ghlog.install(log)
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
    "cards_captcha": 0,      # карточек, закрытых проверочным кодом (fetch_card_checked)
    "cards_blocked": 0,      # карточек-заглушек: портал недоступен/антибот-блок (looks_like_non_card_page)
    "cards_breaker_skipped": 0,  # карточек пропущено предохранителем отключённого суда (card_breaker_allows)
    "movement_odd_width": 0,  # строк «Движения дела» с шириной не по шапке — колонки не разложены (только legacy text)
    "push_sent": 0,          # Web Push: доставлено подписчикам
    "push_failed": 0,        # Web Push: WebPushException (skip по watchlist — не сбой)
    "llm_summary_calls": 0,       # пересказы актов: реальные вызовы LLM
    "llm_summary_cache_hits": 0,  # пересказы актов: взяты из кэша
    "llm_summary_failed": 0,          # пересказы актов: все попытки исчерпаны → откат на excerpt
    "llm_summary_fallback_saved": 0,  # пересказы актов: спасены фолбэк-моделью OpenRouter
    "bank_intake_candidates": 0,  # строк «банк-истец», прошедших строковые фильтры
    "bank_intake_cards": 0,       # карточек кандидатов, прочитанных подхватом
    "bank_intake_added": 0,       # исков банка заведено подхватом за прогон
}


# Состояние пер-суд предохранителя карточек: {хост суда: {"fails": подряд
# не прочитанных карточек, "open": суд отключён, "reason": причина последнего
# фейла, "skipped": пропущено карточек, "probes": half-open проб,
# "preopened": открыт канарейкой (заглушка на странице поиска)}}. Живёт один
# прогон; мутируют только хелперы netutil.card_breaker_* (единая точка),
# runs.py читает для 🩺-алертов блока 4e.
CARD_BREAKER: dict[str, dict] = {}


def _metrics_reset() -> None:
    for k in METRICS:
        METRICS[k] = 0
    CARD_BREAKER.clear()
