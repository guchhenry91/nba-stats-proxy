"""The whole-league game log route (/api/nba/leaguegamelog), stats.nba.com stubbed."""
import app as proxy


class _Log:
    calls = []

    def __init__(self, **kw):
        _Log.calls.append(kw)

    def get_dict(self):
        return {"resultSets": [{"headers": ["PLAYER_ID", "GAME_ID"], "rowSet": [[1, "0012600001"]]}]}


def test_preseason_log_is_passed_through_and_cached(monkeypatch):
    monkeypatch.setattr(proxy.leaguegamelog, "LeagueGameLog", _Log)
    proxy._cache.clear()
    _Log.calls.clear()
    c = proxy.app.test_client()
    url = "/api/nba/leaguegamelog?season=2026-27&season_type=Pre%20Season"
    r = c.get(url)
    assert r.status_code == 200
    assert r.get_json()["resultSets"][0]["rowSet"] == [[1, "0012600001"]]
    assert _Log.calls[0]["season_type_all_star"] == "Pre Season"
    assert _Log.calls[0]["player_or_team_abbreviation"] == "P"
    c.get(url)
    assert len(_Log.calls) == 1                 # second ask served from the cache


def test_bad_arguments_are_refused(monkeypatch):
    monkeypatch.setattr(proxy.leaguegamelog, "LeagueGameLog", _Log)
    c = proxy.app.test_client()
    assert c.get("/api/nba/leaguegamelog?season_type=All%20Star").status_code == 400
    assert c.get("/api/nba/leaguegamelog?season=2026").status_code == 400
