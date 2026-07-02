# Пакетный маркер: даёт тестам scripts/tests уникальное пакетное имя
# (scripts.tests), чтобы pytest мог собирать их вместе с корневым tests/
# без коллизии «два пакета tests» (ModuleNotFoundError при общем прогоне).
