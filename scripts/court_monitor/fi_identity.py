"""Единая проверка суда первой инстанции без сети и изменения записей."""

from __future__ import annotations

from dataclasses import dataclass
import re

from court_monitor.courts import canon_sudrf_domain, match_region_first_instance
from court_monitor.regions import get_region
from court_monitor.regions.base import CourtConfig, RegionConfig, _eyo


@dataclass(frozen=True)
class FiIdentity:
    status: str
    domain: str = ""
    srv_num: int | None = None
    judicial_uid: str = ""
    court: CourtConfig | None = None
    reason: str = ""


def case_identity_fi(case: dict) -> dict:
    """FI-идентичность с проверкой известных УИД всех инстанций, без мутации."""
    fi = dict(case.get("first_instance") or {})
    uids = {((case.get(block) or {}).get("judicial_uid") or "").strip()
            for block in ("first_instance", "appeal", "cassation")} - {""}
    if len(uids) > 1:
        fi["_identity_conflict"] = True
    elif uids:
        fi["judicial_uid"] = next(iter(uids))
    return fi


def normalize_court_name(name: str) -> str:
    return re.sub(r"\s+", " ", _eyo((name or "").strip().lower()))


_REGION_NAME = re.compile(
    r"[\w-]+\s+(?:области|область|края|край|автономного\s+округа|автономный\s+округ)\b"
    r"|\bреспублик[аи]\s+[\w-]+"
)


def _region_marker_matches(marker: str, area: str) -> bool:
    marker = normalize_court_name(marker)
    if len(marker) <= 4:
        return bool(re.search(r"(?<!\w)" + re.escape(marker) + r"(?!\w)", area))
    return marker in area


def _server(value) -> int | None:
    try:
        return int(value) if int(value) > 0 else None
    except (TypeError, ValueError):
        return None


def resolve_fi_identity(fi: dict | None, *, region: RegionConfig | None = None) -> FiIdentity:
    """Домен, уникальное имя/алиас, затем полное имя с проверкой региона.

    На общем домене отсутствие площадки не заменяется первой записью реестра.
    Непустой домен не переписывается по имени: противоречие требует проверки.
    ``court`` нужен для параметров адреса; его отсутствие не стирает явный домен.
    """
    fi = fi or {}
    region = region or get_region()
    domain = canon_sudrf_domain(fi.get("court_domain"))
    srv = _server(fi.get("srv_num"))
    uid = (fi.get("judicial_uid") or "").strip()
    if fi.get("_identity_conflict"):
        return FiIdentity("needs_review", domain, srv, uid,
                          reason="УИД инстанций противоречат друг другу")
    if fi.get("magistrate"):
        return FiIdentity("magistrate", domain, srv, uid, reason="мировой судья")
    name = normalize_court_name(fi.get("court") or "")
    # Явная чужая территория в полном имени — противоречие даже тогда,
    # когда местный matcher (правильно) не нашёл это имя. Проверяем именно
    # обозначение региона: «Ханты-Мансийский» бывает частью имени райсуда.
    if domain and any(not any(_region_marker_matches(marker, area)
                             for marker in region.fi_region_markers)
                      for area in _REGION_NAME.findall(name)):
        return FiIdentity("needs_review", domain, srv, uid,
                          reason="регион имени суда противоречит территории")
    registry = (*region.first_instance_courts, *region.appeal_courts)
    named = [c for c in registry if any(
        normalize_court_name(n) == name
        for n in (c.name, *getattr(c, "name_aliases", ()))
    )] if name else []
    named_domains = {canon_sudrf_domain(c.domain) for c in named}
    if len(named_domains) > 1:
        return FiIdentity("needs_review", domain, srv, uid, reason="неоднозначное имя суда")
    if not named and name:
        long_match = match_region_first_instance(fi.get("court") or "", region)
        if long_match is not None:
            # Общий matcher определяет домен; полное имя может дополнительно
            # прямо называть отдельную площадку этого домена.
            named_sites = [c for c in registry
                           if canon_sudrf_domain(c.domain) == canon_sudrf_domain(long_match.domain)
                           and "(" in c.name and any(
                               normalize_court_name(n) in name
                               for n in (c.name, *getattr(c, "name_aliases", ())))]
            named = named_sites or [long_match]
            named_domains = {canon_sudrf_domain(long_match.domain)}
    if domain and named_domains and domain not in named_domains:
        return FiIdentity("needs_review", domain, srv, uid, reason="домен противоречит имени суда")
    if not domain and named_domains:
        domain = next(iter(named_domains))
    if not domain:
        return FiIdentity("needs_review", judicial_uid=uid, reason="суд не определён")
    candidates = [c for c in registry if canon_sudrf_domain(c.domain) == domain]
    sites = {c.srv_num for c in candidates}
    if len(sites) > 1 and srv is not None and srv not in sites:
        return FiIdentity("needs_review", domain, srv, uid,
                          reason="неизвестная площадка общего домена")
    explicit_sites = {c.srv_num for c in named if "(" in c.name}
    if len(explicit_sites) > 1 or (srv is not None and explicit_sites and srv not in explicit_sites):
        return FiIdentity("needs_review", domain, srv, uid,
                          reason="площадка противоречит имени суда")
    # Явное имя отдельной площадки достаточно; общее имя суда — нет.
    if srv is None and len(sites) > 1:
        if len(explicit_sites) == 1:
            srv = next(iter(explicit_sites))
        else:
            return FiIdentity("needs_review", domain, None, uid,
                              reason="не определена площадка общего домена")
    if srv is None and len(sites) == 1:
        srv = next(iter(sites))
    court = next((c for c in candidates if srv is None or c.srv_num == srv), None)
    return FiIdentity("resolved", domain, srv, uid, court)


def compare_fi_identity(left_fi: dict | None, right_fi: dict | None, *,
                        region: RegionConfig | None = None) -> str:
    """Сравнить суд/площадку/известный УИД; номер сравнивает вызывающий код."""
    region = region or get_region()
    left = resolve_fi_identity(left_fi, region=region)
    right = resolve_fi_identity(right_fi, region=region)
    if left.status == "magistrate" or right.status == "magistrate":
        return "different"
    conflict = ("противореч", "неоднозначное имя")
    if any(any(word in item.reason for word in conflict) for item in (left, right)):
        return "needs_review"
    if left.domain and right.domain and left.domain != right.domain:
        return "different"
    if left.status != "resolved" or right.status != "resolved":
        return "needs_review"
    known_sites = {c.srv_num for c in region.first_instance_courts
                   if canon_sudrf_domain(c.domain) == left.domain}
    if (len(known_sites) > 1 and left.srv_num and right.srv_num
            and left.srv_num != right.srv_num):
        return "different"
    if (left.judicial_uid and right.judicial_uid
            and left.judicial_uid != right.judicial_uid):
        return "needs_review"
    return "same"


class FiDedupIndex(set):
    """Совместимое множество пар с площадкой и УИД исходной записи."""

    def __init__(self, values=()):
        super().__init__(values)
        self.identities: dict[tuple[str, str], list[dict]] = {}

    def add_identity(self, domain: str, number: str, fi: dict) -> None:
        key = (canon_sudrf_domain(domain), number)
        super().add(key)
        self.identities.setdefault(key, []).append(dict(fi))

    def discard(self, key) -> None:
        super().discard(key)
        self.identities.pop(key, None)


class FiUncertainIndex(set):
    def __init__(self):
        super().__init__()
        self.identities: dict[str, list[dict]] = {}

    def add_identity(self, number: str, fi: dict) -> None:
        super().add(number)
        self.identities.setdefault(number, []).append(dict(fi))


def fi_number_tracking_status(number: str, domain: str, exact: set, wildcard: set,
                              *, srv_num=None, judicial_uid: str = "") -> str:
    """``tracked`` — подтверждённый дубль; неизвестность — ``needs_review``."""
    number = (number or "").strip()
    forms = {number, number.split("(")[0].strip()} - {""}
    domain = canon_sudrf_domain(domain)
    requested = {"court_domain": domain, "srv_num": srv_num,
                 "judicial_uid": judicial_uid}
    candidates = []
    for form in forms:
        key = (domain, form)
        if key in exact:
            candidates.extend(getattr(exact, "identities", {}).get(key) or
                              [{"court_domain": domain}])
        if form in wildcard:
            candidates.extend(getattr(wildcard, "identities", {}).get(form) or [{}])
    same, uncertain = [], False
    for candidate in candidates:
        verdict = compare_fi_identity(requested, candidate)
        if verdict == "same":
            same.append(candidate)
        elif verdict == "needs_review":
            uncertain = True
    uids = {(fi.get("judicial_uid") or "").strip() for fi in same} - {""}
    if uncertain or len(uids) > 1:
        return "needs_review"
    return "tracked" if same else "free"
