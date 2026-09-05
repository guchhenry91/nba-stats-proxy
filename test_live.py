"""What the live shaper must and must not let through.

Run: python -m pytest test_live.py -q   (pure stdlib + pytest, no service deps)

THE FIXTURE IS REAL. It is the payload the 2026-09-05 probe actually returned --
Newcastle 0-1 Bournemouth at 1H 28' in league 39 -- not a shape invented to match
the parser. A parser tested only against its author's idea of the payload is
tested against nothing.
"""
import pytest

import live

# Trimmed from the real /fixtures?live=all response.
REAL = {
    "fixture": {"id": 1557395, "referee": "R. Jones", "timezone": "UTC",
                "date": "2026-09-05T11:30:00+00:00",
                "venue": {"id": 562, "name": "St. James' Park"},
                "status": {"long": "First Half", "short": "1H", "elapsed": 28}},
    "league": {"id": 39, "name": "Premier League", "country": "England",
               "season": 2026, "round": "Regular Season - 3"},
    "teams": {"home": {"id": 34, "name": "Newcastle"},
              "away": {"id": 35, "name": "Bournemouth"}},
    "goals": {"home": 0, "away": 1},
}


def _with(**over):
    row = {k: (dict(v) if isinstance(v, dict) else v) for k, v in REAL.items()}
    for key, value in over.items():
        row[key] = value
    return row


def test_the_real_payload_shapes_into_a_card():
    out = live.shape([REAL])
    assert len(out) == 1
    row = out[0]
    assert row["league"] == "PL"
    assert (row["home"], row["away"]) == ("Newcastle", "Bournemouth")
    assert (row["home_goals"], row["away_goals"]) == (0, 1)
    assert (row["status"], row["elapsed"]) == ("1H", 28)


def test_a_goalless_match_is_not_mistaken_for_missing_data():
    """0 is a real score. `goals.get('home') or 0` would render a genuine 0-0 the
    same as a row with no goals at all, and the difference matters on a card."""
    out = live.shape([_with(goals={"home": 0, "away": 0})])
    assert out[0]["home_goals"] == 0 and out[0]["away_goals"] == 0


def test_other_competitions_are_dropped():
    """One request returns every live match on earth -- 102 of them on the day
    this was written, of which one was ours."""
    out = live.shape([_with(league={"id": 170, "name": "League One"}),
                      _with(league={"id": 696, "name": "U18 Premier League"}),
                      REAL])
    assert [r["league"] for r in out] == ["PL"]


@pytest.mark.parametrize("status", sorted(live.IN_PLAY))
def test_every_in_play_status_is_kept(status):
    out = live.shape([_with(fixture={**REAL["fixture"],
                                     "status": {"short": status, "elapsed": 45}})])
    assert len(out) == 1, f"{status} should count as in play"


@pytest.mark.parametrize("status", sorted(live.FINISHED | live.NOT_STARTED))
def test_finished_and_unstarted_are_never_live(status):
    """THE ONE THAT KEEPS THIS HONEST IN BOTH DIRECTIONS. A postponed match shown
    as in progress is a lie to the reader; a FINISHED match leaking through here
    would put a provisional-looking score next to a result the grading pipeline
    owns, and grading settles on FT/AET/PEN alone."""
    out = live.shape([_with(fixture={**REAL["fixture"],
                                     "status": {"short": status, "elapsed": None}})])
    assert out == [], f"{status} must not appear as live"


def test_a_row_missing_its_teams_is_dropped_not_blanked():
    assert live.shape([_with(teams={})]) == []
    assert live.shape([_with(teams={"home": {"name": "Newcastle"}})]) == []


@pytest.mark.parametrize("junk", [None, [], [None], ["x"], [{}], [{"league": {}}]])
def test_junk_never_raises(junk):
    """This runs inside a web request on every poll; an exception here is a 500
    on the reader's page for as long as the upstream is malformed."""
    assert live.shape(junk) == []


def test_every_configured_league_has_a_code():
    assert set(live.LEAGUES.values()) == {"PL", "LALIGA", "BUNDESLIGA",
                                          "LIGUE1", "SERIEA", "UCL"}


def test_in_play_and_finished_never_overlap():
    assert not (live.IN_PLAY & live.FINISHED)
    assert not (live.IN_PLAY & live.NOT_STARTED)
