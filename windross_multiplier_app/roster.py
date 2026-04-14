import os
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import requests
from pypdf import PdfReader
from io import BytesIO


@dataclass
class Team:
    team_id: str
    team_name: str
    player_a: str
    player_b: str


_ROSTER_CACHE = {"expires_at": 0.0, "value": None}


def _apply_mojibake_fixes_to_teams(teams: List[Team]) -> List[Team]:
    """In case we already cached a mojibake-containing roster, fix it before returning."""
    for t in teams:
        try:
            t.team_name = _fix_mojibake_team_name(t.team_name)
        except Exception:
            # Never break roster loading for a single bad label.
            pass
    return teams


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _fix_mojibake_team_name(team_name: str) -> str:
    """Fix common Google Sheets CSV mojibake where UTF-8 chars are rendered as Latin-1.

    Examples observed:
      - 'CÂ²'  -> 'C²'
      - 'TMWÂ®' -> 'TMW®'
    """
    if not team_name:
        return team_name

    # Targeted replacements only (avoid stripping legitimate 'Â' characters).
    return (
        team_name
        .replace("Â²", "²")
        .replace("Â®", "®")
        .replace("Â™", "™")
        .replace("Â©", "©")
    )


def _team_id_from_players(a: str, b: str) -> str:
    na = _normalize_ws(a).lower()
    nb = _normalize_ws(b).lower()
    if na <= nb:
        return f"{na}|{nb}"
    return f"{nb}|{na}"


def _parse_team_name_and_player_pair_from_tokens(tokens: List[str]) -> Optional[Tuple[str, str, str]]:
    """Return (team_name, player_a, player_b) for a tokenized roster row."""
    if len(tokens) < 6:
        return None

    particles = {"de", "du", "van", "von"}

    def parse_person(start: int) -> Optional[Tuple[str, int]]:
        if start + 1 >= len(tokens):
            return None
        first = tokens[start]
        second = tokens[start + 1]
        if second.lower() in particles and start + 2 < len(tokens):
            person = f"{first} {tokens[start + 1]} {tokens[start + 2]}".strip()
            return person, start + 3
        person = f"{first} {second}".strip()
        return person, start + 2

    p1 = parse_person(0)
    if not p1:
        return None
    player_a, next_i = p1

    p2 = parse_person(next_i)
    if not p2:
        return None
    player_b, next_i2 = p2

    remaining = tokens[next_i2:]
    if not remaining:
        return None
    team_name = " ".join(remaining).strip()
    return team_name, player_a, player_b


def _download_roster_pdf(pdf_url: str) -> bytes:
    resp = requests.get(pdf_url, timeout=60)
    resp.raise_for_status()
    return resp.content


def load_roster_from_google_standings(*, force_refresh: bool = False) -> List[Team]:
    """Load 2v2 team roster from the Windross standings source.

    Preferred source: published Google Sheets CSV embed (preserves symbols like superscripts/reg marks).
    Fallback: parse the PDF standings (may lose superscripts/symbols).
    """
    cache_seconds = int(os.getenv("WINDROSS_ROSTER_CACHE_SECONDS", "3600"))
    now = time.time()
    if force_refresh:
        _ROSTER_CACHE["expires_at"] = 0.0
    if _ROSTER_CACHE["value"] is not None and _ROSTER_CACHE["expires_at"] > now:
        # Ensure we don't return previously-cached mojibake labels.
        return _apply_mojibake_fixes_to_teams(_ROSTER_CACHE["value"])

    # 1) Try published Google Sheets CSV (preserves rich text as best as possible)
    # Example default derived from the Google Sites embed used on the Teams Registered page.
    csv_url = os.getenv(
        "WINDROSS_ROSTER_SHEET_CSV_URL",
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRiYG7srKcznWImnUpzZmQzrqYN9hw-EcYU-O1SRGEsBkCdYyFwtb_8Ha8dOqEZ5uke21HADH13lwzA/pub?output=csv&gid=1027047095",
    )

    teams: List[Team] = []
    seen = set()

    try:
        resp = requests.get(csv_url, timeout=60)
        resp.raise_for_status()
        import csv
        import io

        text = resp.text

        # Google Sheets CSV exports sometimes include a junk first line (emoji/title), so
        # we locate the real header row that contains the expected column names.
        lines = [ln for ln in text.splitlines() if ln.strip()]
        header_idx = None
        for i, ln in enumerate(lines):
            if "Team Name" in ln and "Player 1" in ln and "Player 2" in ln:
                header_idx = i
                break
        if header_idx is None:
            raise ValueError("Could not find expected CSV header row")

        csv_text = "\n".join(lines[header_idx:])
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            if not row:
                continue

            team_name = (row.get("Team Name") or "").strip()
            player_a = (row.get("Player 1") or "").strip()
            player_b = (row.get("Player 2") or "").strip()

            if not team_name or not player_a or not player_b:
                continue

            # Fix common Google Sheets CSV mojibake for special glyphs.
            team_name = _fix_mojibake_team_name(team_name)

            team_id = _team_id_from_players(player_a, player_b)
            if team_id in seen:
                continue
            seen.add(team_id)
            teams.append(Team(team_id=team_id, team_name=team_name, player_a=player_a, player_b=player_b))

        if teams:
            _ROSTER_CACHE["value"] = teams
            _ROSTER_CACHE["expires_at"] = now + cache_seconds
            return _apply_mojibake_fixes_to_teams(teams)
    except Exception:
        # Fall through to PDF parsing.
        teams = []
        seen = set()

    # 2) Fallback: PDF standings parsing (may lose superscripts/symbols)
    pdf_id = os.getenv("WINDROSS_ROSTER_PDF_ID", "1rapi4XlbA_dPSjq65lPwzqV4yGa5TPby")
    pdf_url = os.getenv(
        "WINDROSS_ROSTER_PDF_URL",
        f"https://drive.google.com/uc?export=download&id={pdf_id}",
    )
    pdf_bytes = _download_roster_pdf(pdf_url)
    pdf = PdfReader(BytesIO(pdf_bytes))
    text = "\n".join((page.extract_text() or "") for page in pdf.pages)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    row_lines = []
    for ln in lines:
        if re.search(r"\d", ln) or "%" in ln or "N/A" in ln:
            row_lines.append(ln)

    for ln in row_lines:
        tokens = re.split(r"\s+|,|;|\|", ln)
        if len(tokens) < 6:
            continue

        stats_start = None
        for i, tok in enumerate(tokens):
            if re.search(r"\d", tok):
                stats_start = i
                break
            if tok.upper() == "N/A":
                stats_start = i
                break

        if stats_start is None or stats_start < 5:
            continue

        left = tokens[:stats_start]
        parsed_team = _parse_team_name_and_player_pair_from_tokens(left)
        if not parsed_team:
            continue
        team_name, player_a, player_b = parsed_team

        team_id = _team_id_from_players(player_a, player_b)
        if team_id in seen:
            continue
        seen.add(team_id)
        teams.append(Team(team_id=team_id, team_name=team_name, player_a=player_a, player_b=player_b))

    _ROSTER_CACHE["value"] = teams
    _ROSTER_CACHE["expires_at"] = now + cache_seconds
    return teams

