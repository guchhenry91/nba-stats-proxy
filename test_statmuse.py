"""StatMuse game logs are read by COLUMN HEADER from the ask-by-name page (2026-10-20).

Reading by position broke silently: NBA fg3m was field goals made, MLB homeRuns was
doubles, NHL shots was penalty minutes; hand-written player ids opened other players."""
from pathlib import Path

import app

FIX = Path(__file__).parent / "fixtures"


def _rows(name):
    return app._sm_table((FIX / name).read_text(encoding="utf-8"))


def test_nba_threes_are_the_3pm_column_not_field_goals():
    r = _rows("statmuse_nba_ask.html")[0]
    assert r["HA"] in ("vs", "@") and "Brunson" in r["NAME"]
    assert float(r["3PM"]) <= float(r["FGM"])          # threes can never exceed field goals
    assert r["3PM"] != r["FGM"] or r["FGM"] == "0"


def test_mlb_home_runs_and_strikeouts_by_header():
    r = _rows("statmuse_mlb_ask.html")[0]
    assert {"AB", "H", "2B", "3B", "HR", "RBI", "BB", "SO"} <= set(r)
    assert "Judge" in r["NAME"]


def test_nhl_shots_and_toi_by_header():
    r = _rows("statmuse_nhl_ask.html")[0]
    assert {"G", "A", "P", "S", "PIM", "PPG", "PPA", "TOI"} <= set(r)
    assert "McDavid" in r["NAME"] and ":" in r["TOI"]


def test_a_page_for_another_player_is_refused(monkeypatch):
    html = (FIX / "statmuse_nba_ask.html").read_text(encoding="utf-8")

    class Resp:
        status_code, text = 200, html
    monkeypatch.setattr(app.http_requests, "get", lambda *a, **k: Resp())
    rows, err = app._sm_gamelog("nba", "Someone Else", 5)
    assert rows is None and err[1] == 404
    rows, err = app._sm_gamelog("nba", "Jalen Brunson", 5)
    assert err is None and len(rows) == 5


def test_season_labels():
    nba = app._sm_seasons("nba")
    assert len(nba) == 2 and "-" in nba[0]
    assert app._sm_seasons("mlb")[0].isdigit()
