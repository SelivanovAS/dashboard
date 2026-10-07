#!/usr/bin/env python3
"""Финальный технический отчёт workflow или служебный сигнал VPS.

Без --send печатает HTML и ничего не отправляет. Нет зависимостей pip.
"""
from __future__ import annotations
import argparse
import sys

from court_monitor import technical_report as report


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == 'ops':
        parser = argparse.ArgumentParser(description='Служебные уведомления исполнителя')
        parser.add_argument('--event', required=True, choices=['parse-result', 'retry-result', 'watchdog', 'failure'])
        parser.add_argument('--repo', required=True)
        parser.add_argument('--telegram-config', required=True)
        parser.add_argument('--message', default='')
        from court_monitor import technical_report_ops
        return technical_report_ops.main(parser.parse_args(argv[1:]))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--send', action='store_true')
    parser.add_argument('--workflow-status', default='unknown', choices=['success', 'failure', 'cancelled', 'unknown'])
    parser.add_argument('--data-publication', default='not_confirmed', choices=['confirmed', 'failed', 'not_confirmed', 'skipped'])
    parser.add_argument('--pages', default='unconfirmed', choices=['confirmed', 'unconfirmed', 'failed', 'skipped'])
    parser.add_argument('--push-step', default='unknown', choices=['success', 'failure', 'cancelled', 'skipped', 'unknown'])
    parser.add_argument('--digest-step', default='unknown', choices=['success', 'failure', 'cancelled', 'skipped', 'unknown'])
    args = parser.parse_args(argv)
    value = report.finalize(workflow_status=args.workflow_status, data_publication=args.data_publication,
                            pages=args.pages, push_step=args.push_step, digest_step=args.digest_step)
    if args.send:
        ok = report.send(value)
        print('Технический отчёт принят Telegram.' if ok else 'Технический отчёт не подтверждён Telegram; JSON сохранён.')
        return 0 if ok else 1
    print(report.render(value))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
