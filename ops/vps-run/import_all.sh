#!/bin/bash
# =============================================================================
# VPS: дневная очередь операторских импортов — тонкий шим над БОЕВЫМ
# ops/mac-local-run/import_all.sh → import_dumps.sh (дампы + точечные пачки).
# Запускается systemd-таймером court-import.timer (слоты — зеркало
# com.court-monitor.import.plist); руками: bash ops/vps-run/import_all.sh [--dry-run]
# =============================================================================
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/vps_env.sh"
exec bash "$HERE/../mac-local-run/import_all.sh" --anywhere "$@"
