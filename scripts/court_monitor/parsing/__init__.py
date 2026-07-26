# -*- coding: utf-8 -*-
"""Парсеры sudrf.ru: таблицы, поисковая выдача, карточки дел, кассация 7kas.

Публичные имена ре-экспортируются здесь — импортёрам не нужно знать
внутреннюю нарезку на подмодули.
"""

from court_monitor.parsing.tables import (  # noqa: F401
    TableExtractor, extract_tables, cell_text, cell_href,
)
from court_monitor.parsing.search import (  # noqa: F401
    _parse_combined_cell, _SBER_SUBSIDIARY_PATTERNS,
    is_subsidiary_only_case, is_insurance_only_case, _is_real_sberbank,
    determine_bank_role_from_participants, parties_from_participants,
    parse_search_page, _find_results_table, parse_first_instance_search,
    find_fi_case_link, detect_captcha_challenge, detect_captcha_challenge_card,
    looks_like_non_card_page, is_no_data_page,
)
from court_monitor.parsing.cards import (  # noqa: F401
    _extract_act_text, _warn_if_card_degraded, card_is_empty_shell,
    parse_case_card, fetch_act_text,
)
from court_monitor.parsing.cassation import (  # noqa: F401
    _CASS_CATEGORY_RE, _CASS_CASSATOR_RE, _CASS_FI_COURT_RE,
    _CASS_FI_CASE_NUM_RE, _CASS_INTERNAL_NUM_RE,
    parse_cassation_search_page,
    _CASS_ACT_DIV_RE, _CASS_ACT_DELO_NUM_RE, _extract_cassation_act_text,
    classify_cassation_outcome, cassation_remanded_to, CASSATION_OUTCOME_RU,
    _extract_cassation_terminated_reason, cassation_terminated_label,
    cassation_review_label, parse_cassation_card,
)
