# -*- coding: utf-8 -*-
"""GitHub Actions: сворачиваемые группы фаз и аннотации WARNING/ERROR.

Включается ТОЛЬКО явным env LOG_GH_ANNOTATIONS=1 (его ставят шаги Run в
update_cases.yml / test_digest.yml / replay_on_push.yml). Голый гейт по
GITHUB_ACTIONS нельзя: tests.yml гоняет pytest в Actions, тесты намеренно
провоцируют WARNING/ERROR — получились бы мусорные аннотации у тестовых
прогонов. Без env все функции — no-op, вывод байт-в-байт прежний
(Mac-резерв и локальные прогоны не затронуты).

Workflow-команды (::group::, ::warning:: и т.п.) печатаются напрямую через
print(..., flush=True), НЕ через логгер: префикс логгера «12:34:56 [INFO]»
сдвинул бы `::` из начала строки, и GitHub команду не распознал бы.

Модуль — только stdlib и не импортирует config (его самого импортирует
config при старте — циклов нет).
"""

from __future__ import annotations

import logging
import os

# Обрезка текста аннотации: в панели Annotations длинные простыни не нужны,
# полный текст всегда есть в соседней обычной строке лога.
_ANNOTATION_MAX_LEN = 800


def enabled() -> bool:
    """Аннотации/группы включены? (env LOG_GH_ANNOTATIONS=1)."""
    return os.environ.get("LOG_GH_ANNOTATIONS") == "1"


def gh_escape(msg: str) -> str:
    """Экранирование текста workflow-команды GitHub.

    Порядок замен важен: сначала %, иначе %0A из перевода строки
    заэкранируется повторно.
    """
    msg = msg.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    if len(msg) > _ANNOTATION_MAX_LEN:
        msg = msg[:_ANNOTATION_MAX_LEN] + "…"
    return msg


class _AnnotationHandler(logging.Handler):
    """Дублирует WARNING/ERROR-записи аннотациями ::warning::/::error::.

    Именно дубль-строка, а не замена формата: обычная строка лога остаётся
    многострочной и читаемой, аннотация собирается только из текста записи
    (без времени/уровня/трейсбека).
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)

    def emit(self, record: logging.LogRecord) -> None:
        kind = "error" if record.levelno >= logging.ERROR else "warning"
        print(f"::{kind}::{gh_escape(record.getMessage())}", flush=True)


# Открыта ли сейчас группа (фазы прогона строго последовательны,
# вложенных групп GitHub не поддерживает).
_group_open = False


def start_group(title: str) -> None:
    """Открыть сворачиваемую группу, закрыв предыдущую."""
    if not enabled():
        return
    end_group()
    print(f"::group::{gh_escape(title)}", flush=True)
    global _group_open
    _group_open = True


def end_group() -> None:
    """Закрыть текущую группу (идемпотентно; вне группы — no-op)."""
    global _group_open
    if not enabled() or not _group_open:
        return
    print("::endgroup::", flush=True)
    _group_open = False


def install(logger: logging.Logger) -> None:
    """Повесить обработчик аннотаций на логгер (если включены)."""
    if enabled():
        logger.addHandler(_AnnotationHandler())
