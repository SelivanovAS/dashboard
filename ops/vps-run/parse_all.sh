#!/bin/bash
# =============================================================================
# VPS: утренний прогон всех территорий — тонкий шим над БОЕВЫМ Mac-звеном.
# Логика целиком в ops/mac-local-run/parse_all.sh → parse_and_push.sh
# (слоты, транзакции, гейт «один дайджест в день», окно 08:45, sweep,
# импорты после парсеров) — здесь только Linux-окружение и --anywhere.
# Запускается systemd-таймером court-parse.timer (слоты — зеркало
# com.court-monitor.parse.plist); руками: bash ops/vps-run/parse_all.sh [--check]
# =============================================================================
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/vps_env.sh"
exec bash "$HERE/../mac-local-run/parse_all.sh" --anywhere "$@"
