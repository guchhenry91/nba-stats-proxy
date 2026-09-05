"""Shaping and filtering for live in-play scores. Pure stdlib, no Flask.

SEPARATE FROM app.py ON PURPOSE. The rules here -- which competitions count, and
what "in play" actually means -- are the load-bearing part of the live feature,
and they were unreachable by a test while they sat inside a route body: app.py
imports nba_api and understatapi, so testing one dict comprehension meant
installing the whole service.

WHAT THIS IS FOR. index.html is served as a STATIC site, so it has nowhere to
keep an API key and no way to update a file without a deploy. Live scores need
~60s granularity against a publish pipeline that gets 24 rationed CI slots a day.
The page therefore fetches live data from a running origin instead, and this is
the shaping that origin applies.

NOTHING HERE SETTLES ANYTHING. These scores are provisional by definition. The
engine grades on FT/AET/PEN alone; a live score written as final would settle a
pick against a scoreline that had not happened yet, into an append-only record.
"""

# API-Football league ids: the five domestic leagues the engine models, plus the
# Champions League. Everything else in play worldwide is dropped -- on 2026-09-05
# a single request returned 102 live fixtures and one of them was ours, so this
# filter is most of the work.
LEAGUES = {39: "PL", 140: "LALIGA", 78: "BUNDESLIGA",
           61: "LIGUE1", 135: "SERIEA", 2: "UCL"}

# Statuses where a ball is actually in play, INCLUDING half time and breaks --
# the match is running, it just is not being played this second.
#
# DELIBERATELY NOT "anything that is not finished". NS, TBD, PST and CANC are all
# unfinished and none is live; a card claiming a postponed match is in progress
# is worse than showing nothing. And FT/AET/PEN are finished results the grading
# pipeline owns, so they are excluded from the other side too.
IN_PLAY = {"1H", "HT", "2H", "ET", "BT", "P", "LIVE", "INT", "SUSP"}

FINISHED = {"FT", "AET", "PEN"}          # owned by grading; never "live"
NOT_STARTED = {"TBD", "NS", "PST", "CANC", "ABD", "AWD", "WO"}


def shape(response) -> list:
    """API-Football's /fixtures?live=all payload -> the rows a live card needs.

    Unknown competitions and anything not actually in play are dropped. A row
    missing the fields a card needs is dropped too rather than rendered with
    blanks -- half a scoreline is worse than none.
    """
    out = []
    for row in response or []:
        if not isinstance(row, dict):
            continue
        league = row.get("league") or {}
        code = LEAGUES.get(league.get("id"))
        if not code:
            continue
        fixture = row.get("fixture") or {}
        status = fixture.get("status") or {}
        if status.get("short") not in IN_PLAY:
            continue
        teams, goals = row.get("teams") or {}, row.get("goals") or {}
        home = (teams.get("home") or {}).get("name")
        away = (teams.get("away") or {}).get("name")
        if not home or not away:
            continue
        out.append({
            "id": fixture.get("id"),
            "league": code,
            "home": home,
            "away": away,
            # A goal count of 0 is meaningful and must survive; only None is
            # missing. `or 0` would render a real 0-0 identically to no data.
            "home_goals": goals.get("home"),
            "away_goals": goals.get("away"),
            "status": status.get("short"),
            "elapsed": status.get("elapsed"),
            "kickoff": fixture.get("date"),
        })
    return out
