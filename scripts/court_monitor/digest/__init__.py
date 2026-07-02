# -*- coding: utf-8 -*-
"""Дайджест: LLM-вызовы и промпты (llm), пост-обработка HTML (postprocess),
программный рендер (template), диспетчер и контекст (core).

Публичные имена ре-экспортируются здесь.
"""

from court_monitor.digest.llm import (  # noqa: F401
    _gigachat_access_token, GIGACHAT_SYSTEM_PROMPT,
    _normalize_markdown_to_telegram_html, _drop_empty_count_sections,
    _call_gigachat, _ACT_KIND_BY_STAGE, _build_act_summary_prompt,
    _call_claude_simple, _call_gigachat_simple, _SUMMARY_PREFIX_RE,
    _clean_summary, summarize_act_motivation,
    _DIGEST_POLISH_SYSTEM_PROMPT, _FORBIDDEN_TAGS_RE, _collect_case_numbers,
    _validate_polished_html, polish_digest_html,
    _call_claude_polish, _call_gigachat_polish, _current_digest_model_name,
)
from court_monitor.digest.postprocess import (  # noqa: F401
    _DIGEST_CASE_LINK_RE, _SUBSECTION_NUM_PREFIX, _DIGEST_HEADER_RE,
    _BARE_CASE_NUMBER_RE, _FI_BLOCK_HEADER_RE, _APPEAL_BLOCK_HEADER_RE,
    _CASSATION_BLOCK_HEADER_RE, _APPEAL_NUM_RE,
    _line_has_case_number, _wrap_all_bare_case_numbers,
    _wrap_bare_number_in_link, _ensure_appeal_new_case_full_layout,
    _validate_digest_new_sections, _drop_hallucinated_from_section,
    _SUBSECTION_HEADERS_WITH_COUNT, _renumber_section_headers, _classify_line,
    _FOOTER_BADGE_RE, _DASHBOARD_LINK_RE, _ensure_footer,
    _normalize_section_spacing, _count_digest_subsections,
    _DIGEST_SUMMARY_NEW_LABELS, _DIGEST_SUMMARY_STAGE_LABELS,
    summarize_digest_counters, _plural_ru, _compute_summary_lines,
    _SUMMARY_HEADER_RE, _SUMMARY_END_RE, _replace_summary_block,
    _LIST_PRINT_FACTS_FOR_LOG, _warn_misplaced_appeal_cases,
    _shorten_categories_in_html, _drop_zero_count_sections,
    _strip_section_numbering, _purge_3_6_without_act_text,
    _close_open_tags, _strip_orphan_close_tags, truncate_html_message,
)
from court_monitor.digest.template import (  # noqa: F401
    _bank_in_parties, _section_break, next_tuesday, build_summary_line,
    short_category_chain, category_short, _render_act_summary_or_excerpt,
    load_last_meaningful_digest, _format_iso_date_ru,
    render_no_changes_digest, generate_template_digest,
)
