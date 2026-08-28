# Стражи VPS-звена (ops/vps-run) — боевой парсинг на VPS с 28.08.2026.
#
# Главный инвариант: у VPS НЕТ своей копии логики — шимы обязаны exec'ать
# боевые ops/mac-local-run/{parse_all,import_all}.sh с --anywhere. Копия
# транзакций/гейта/фаз доставки — класс молчаливой поломки, которым резерв
# уже дважды болел (списки файлов данных, домены судов, jq-пейлоады).
import plistlib
import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VPS = ROOT / "ops" / "vps-run"
MAC = ROOT / "ops" / "mac-local-run"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _plist_slots(name: str) -> set[tuple[int, int]]:
    with open(MAC / name, "rb") as f:
        plist = plistlib.load(f)
    return {(d["Hour"], d["Minute"]) for d in plist["StartCalendarInterval"]}


def _timer_slots(name: str) -> set[tuple[int, int]]:
    slots = set()
    for line in _read(VPS / "systemd" / name).splitlines():
        m = re.match(r"OnCalendar=Mon\.\.Fri (\d{2}):(\d{2})$", line.strip())
        if m:
            slots.add((int(m.group(1)), int(m.group(2))))
    return slots


class TestVpsShims(unittest.TestCase):
    def test_bash_syntax(self):
        for script in ("parse_all.sh", "import_all.sh", "vps_env.sh",
                       "shims/netstat"):
            rc = subprocess.run(
                ["bash", "-n", str(VPS / script)], capture_output=True
            )
            self.assertEqual(rc.returncode, 0,
                             f"{script}: {rc.stderr.decode()}")

    def test_shims_exec_the_combat_scripts(self):
        # Шим не имеет права нести свою логику — только exec боевого драйвера
        # с --anywhere (netstat-заглушка гарантирует «не в сети Сбера», и без
        # флага боевой скрипт тихо вышел бы, не спросив суды).
        for shim, target in (("parse_all.sh", "mac-local-run/parse_all.sh"),
                             ("import_all.sh", "mac-local-run/import_all.sh")):
            text = _read(VPS / shim)
            self.assertIn(f'exec bash "$HERE/../{target}" --anywhere "$@"',
                          text, shim)
            self.assertIn('. "$HERE/vps_env.sh"', text, shim)

    def test_scripts_are_executable(self):
        for script in ("parse_all.sh", "import_all.sh", "shims/netstat"):
            self.assertTrue((VPS / script).stat().st_mode & 0o111, script)


class TestVpsEnv(unittest.TestCase):
    def test_routes_disabled_and_shims_first(self):
        text = _read(VPS / "vps_env.sh")
        self.assertIn("export CM_COURT_ROUTES_READY=1", text)
        self.assertIn('export PATH="$VPS_HERE/shims:$PATH"', text)

    def test_timezone_guard(self):
        # Окно доставки 08:45 и производственный календарь считаются по
        # местному времени: сервер не в +0500 обязан отказаться громко.
        text = _read(VPS / "vps_env.sh")
        self.assertIn('"$(date +%z)" != "+0500"', text)
        self.assertIn("exit 1", text)

    def test_no_push_secret_and_no_worker_source(self):
        # PUSH_SECRET в окружении прогона включил бы вторую доставку push —
        # worker.<регион> читается только awk'ом внутри боевых скриптов.
        for script in ("vps_env.sh", "parse_all.sh", "import_all.sh"):
            text = _read(VPS / script)
            self.assertNotIn("PUSH_SECRET", text, script)
            self.assertNotIn("worker.", text, script)

    def test_netstat_shim_is_silent_success(self):
        # Пустой stdout + rc 0 = «не в сети Сбера» для cm_in_sber_network.
        rc = subprocess.run([str(VPS / "shims" / "netstat"), "-rn", "-f",
                             "inet"], capture_output=True)
        self.assertEqual(rc.returncode, 0)
        self.assertEqual(rc.stdout, b"")


class TestVpsTimers(unittest.TestCase):
    # Слоты systemd-таймеров — зеркало launchd-plist'ов Mac: обе платформы
    # обязаны жить в одном расписании (окно 08:45 считает parse_and_push.sh,
    # и его последний слот должен совпадать на Mac и VPS).
    def test_parse_slots_mirror_plist(self):
        self.assertEqual(_timer_slots("court-parse.timer"),
                         _plist_slots("com.court-monitor.parse.plist"))

    def test_import_slots_mirror_plist(self):
        self.assertEqual(_timer_slots("court-import.timer"),
                         _plist_slots("com.court-monitor.import.plist"))

    def test_timers_are_persistent(self):
        # Аналог «догнать проспанный слот» launchd после ребута сервера.
        for name in ("court-parse.timer", "court-import.timer"):
            self.assertIn("Persistent=true", _read(VPS / "systemd" / name))

    def test_services_point_at_existing_shims(self):
        for service, shim in (("court-parse.service", "parse_all.sh"),
                              ("court-import.service", "import_all.sh")):
            text = _read(VPS / "systemd" / service)
            m = re.search(r"ExecStart=/bin/bash (\S+)", text)
            self.assertIsNotNone(m, service)
            # Абсолютный путь юнита канонический (/opt/court-monitor/dashboard),
            # но его ХВОСТ обязан существовать в репозитории.
            self.assertTrue(m.group(1).endswith(f"ops/vps-run/{shim}"), service)
            self.assertTrue((VPS / shim).exists(), shim)


if __name__ == "__main__":
    unittest.main()
