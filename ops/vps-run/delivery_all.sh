#!/bin/bash
# VPS: отдельная доставка готовых утренних выпусков, без парсинга судов.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/vps_env.sh"
exec bash "$HERE/../mac-local-run/delivery_all.sh" --anywhere "$@"
