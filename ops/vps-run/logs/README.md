---
author: Александр Селиванов
---

# Автоматическая очистка журналов VPS

Настройка установлена 11.11.2026 на `195.19.66.234`
(`cv7910989.novalocal`, Ubuntu 24.04, systemd 255, logrotate 3.21.0).
Она действует на уровне сервера, отдельно от регионального пакета программы.
Обновление Git не устанавливает и не отменяет эти настройки.

## Правила хранения

| Журналы | Правило |
|---|---|
| Постоянный системный журнал journald | Около 150 МиБ суммарно, срок до 14 дней; отдельный файл до 8 МиБ, смена файла не реже раза в сутки |
| Временный журнал journald в памяти | Лимит 32 МиБ |
| Основные журналы rsyslog, UFW, fail2ban, nginx и неудачных входов btmp | Ежедневная ротация либо при превышении 10 МиБ; до 7 архивов, `maxage 14`, сжатие |
| Проверка logrotate | Каждый час, точность планировщика до минуты; после простоя пропущенная проверка догоняется |
| Docker-журнал `erknm-main-bot-1` | Уже действовало: `json-file`, `max-size=10m`, `max-file=3`; настройка не менялась |

Предел journald не гарантирует сохранение всех 14 суток: при большом потоке
старые записи удаляются раньше по размеру. Активные файлы и гранулярность
ротации допускают отклонение от 150 МиБ. Это не лимит всего `/var/log`.

Порог 10 МиБ проверяется каждый час, поэтому между проверками файл может
стать больше. Семь архивов — количество файлов, а не гарантированные семь
дней: при частой ротации история будет короче. `maxage 14` проверяется при
очередной ротации соответствующего журнала, а не отдельным ежедневным
удалением. Пустые файлы пропускаются. У rsyslog, UFW, fail2ban и nginx
сохранён `delaycompress`: последний архив сжимается при следующей ротации;
у btmp сжатие выполняется сразу.

## Установленные настройки

`/etc/systemd/journald.conf.d/60-court-retention.conf`:

```ini
[Journal]
SystemMaxUse=150M
SystemMaxFileSize=8M
RuntimeMaxUse=32M
MaxRetentionSec=14day
MaxFileSec=1day
```

Штатная передача сообщений в rsyslog (`ForwardToSyslog=yes`) сохранена.

`/etc/systemd/system/logrotate.timer.d/60-hourly-check.conf`:

```ini
[Timer]
OnCalendar=
OnCalendar=hourly
AccuracySec=1min
```

Пустой `OnCalendar=` сбрасывает прежнее ежедневное расписание.
`Persistent=true` наследуется из штатного таймера. Служба использует
`/etc/logrotate.conf`; остальные правила сохраняют собственную периодичность.
При systemd штатный `/etc/cron.daily/logrotate` пропускает запуск.

В существующих блоках `/etc/logrotate.d/rsyslog`, `ufw`, `fail2ban`, `btmp`
и `nginx` установлены:

```text
daily
rotate 7
maxage 14
maxsize 10M
compress
```

Это параметры блоков, а не самостоятельный полный файл конфигурации.
Сохранены исходные пути журналов, права новых файлов, `missingok`,
`notifempty`, обработчики повторного открытия файлов и применимые
`sharedscripts`/`delaycompress`. В btmp сохранено `create 0660 root utmp`.
Блок rsyslog охватывает `/var/log/syslog`, `mail.log`, `kern.log`,
`auth.log`, `user.log`, `cron.log`; nginx — `/var/log/nginx/*.log`.

## Проверка

Команды выполняются на целевом сервере:

```sh
systemd-analyze cat-config systemd/journald.conf
systemctl cat logrotate.timer
logrotate --debug /etc/logrotate.conf
systemd-analyze verify logrotate.timer logrotate.service
systemctl show logrotate.timer -p ActiveState -p UnitFileState -p NextElapseUSecRealtime
systemctl show logrotate.service -p Result -p ExecMainStatus
journalctl -u logrotate.service -n 30 --no-pager
journalctl -u systemd-journald -n 15 --no-pager
journalctl --disk-usage
du -sh /var/log
df -h /
```

`logrotate --debug` только проверяет правила, не вращает файлы и не меняет
состояние. Норма: таймер `active` и `enabled`, последний запуск службы —
`Result=success`, `ExecMainStatus=0`. Сама oneshot-служба после завершения
может быть `inactive`.

При повторной установке сначала сохранить прежние конфигурации вне каталогов
резервных копий рабочих данных, сверить различия и внести параметры выше,
сохраняя штатные блоки и обработчики. После успешных проверок конфигурации:

```sh
systemctl daemon-reload
systemctl restart systemd-journald
systemctl enable logrotate.timer
systemctl restart logrotate.timer
systemctl start logrotate.service
```

Последняя команда выполняет обычную ротацию по правилам. Не применять
`logrotate --force` ко всем журналам для простой проверки настройки.
После обновления системных пакетов повторно сверять эти пять правил
и оба дополнения конфигурации.

## Границы очистки и откат

Очистка не обращается к `/var/backups`, `/var/lib/court-watch-repair`
и `/var/lib/court-program-maintenance`. Резервные копии не удаляются.
История Git обслуживается [отдельным механизмом сжатия](../history/README.md),
сохраняющим все версии. Ротация файлов прогонов в региональных клонах,
правила APT/dpkg, wtmp и других журналов этой настройкой не изменялись.

Для отката восстановить сохранённые версии пяти файлов logrotate и убрать
только два перечисленных дополнения `60-court-retention.conf` и
`60-hourly-check.conf`. Проверить конфигурацию, выполнить `daemon-reload`,
перезапустить journald и таймер logrotate. Штатную ротацию оставлять включённой.
Откат правил не восстанавливает уже удалённые журналы.

## Результат установки 11.11.2026

В 19:54 по Екатеринбургу проверки logrotate и systemd завершились с кодом 0;
настройки применены, первый ручной запуск logrotate успешен. journald
подтвердил лимит 150 МиБ, занимая 144 МиБ. Таймер включён, ближайшая плановая
проверка назначена на 20:00; на момент проверки её выполнение ещё не наблюдалось.

После настройки `/var/log` занимал около 167 МиБ, диск — 51% (4,7 ГиБ
свободно из 9,8 ГиБ). Это снимок, а не прогноз постоянного размера.
Службы journald, rsyslog, fail2ban, пять таймеров мониторинга и таймер сжатия
истории оставались активны; контейнер бота продолжал работать.
Метаданные 2917 объектов в трёх каталогах резервных копий совпали до и после
установки; полное хеширование содержимого резервных копий не выполнялось.
