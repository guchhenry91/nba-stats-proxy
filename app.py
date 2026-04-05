"""
NBA + NHL Stats Proxy — serves real-time player gamelogs to the betting dashboard.
NBA: Uses nba_api to bypass NBA.com's bot protection.
NHL: Proxies api-web.nhle.com to bypass CORS (no Access-Control-Allow-Origin on gamelog).
Deploy to Render free tier.
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import players
import requests as http_requests
from understatapi import UnderstatClient
import unicodedata
import time


def strip_accents(s):
    """Remove accents: Lafrenière → Lafreniere, Müller → Muller"""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )

app = Flask(__name__)
CORS(app)  # allow dashboard to call from any origin

# In-memory cache — gamelogs don't change after a game ends
_cache = {}
CACHE_TTL = 2 * 60 * 60  # 2 hours


def get_cached(key):
    if key in _cache:
        ts, data = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
    return None


def set_cached(key, data):
    _cache[key] = (time.time(), data)


@app.route("/health")
def health():
    return jsonify({"status": "ok", "cache_size": len(_cache)})


@app.route("/api/nba/player/<player_name>/gamelog")
def nba_gamelog(player_name):
    """
    Returns recent gamelog for an NBA player.
    Query params: season (default 2025-26)
    Response: { player_id, player_name, games: [ { date, pts, reb, ast, stl, blk, tov, fg3m, min, is_home } ] }
    """
    season = request.args.get("season", "2025-26")
    cache_key = f"{player_name.lower()}_{season}"

    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        # Find player
        matches = players.find_players_by_full_name(player_name)
        if not matches:
            # Try last name only
            last_name = player_name.split()[-1]
            matches = players.find_players_by_last_name(last_name)
            if matches:
                # Pick the one whose full name is closest
                target = player_name.lower()
                matches = [m for m in matches if m.get("is_active")]
                if not matches:
                    return jsonify({"error": "Player not found", "player": player_name}), 404

        if not matches:
            return jsonify({"error": "Player not found", "player": player_name}), 404

        player = matches[0]
        player_id = player["id"]

        # Fetch gamelog — this calls stats.nba.com with proper headers
        log = playergamelog.PlayerGameLog(
            player_id=player_id,
            season=season,
            season_type_all_star="Regular Season",
            timeout=15,
        )
        rows = log.get_dict()["resultSets"][0]["rowSet"]
        headers = log.get_dict()["resultSets"][0]["headers"]

        # Build clean response
        games = []
        for row in rows:
            r = dict(zip(headers, row))
            matchup = r.get("MATCHUP", "")
            is_home = "vs." in matchup

            games.append({
                "date": r.get("GAME_DATE"),
                "matchup": matchup,
                "is_home": is_home,
                "min": r.get("MIN", 0),
                "pts": r.get("PTS", 0),
                "reb": r.get("REB", 0),
                "ast": r.get("AST", 0),
                "stl": r.get("STL", 0),
                "blk": r.get("BLK", 0),
                "tov": r.get("TOV", 0),
                "fg3m": r.get("FG3M", 0),
                "fgm": r.get("FGM", 0),
                "ftm": r.get("FTM", 0),
                "oreb": r.get("OREB", 0),
                "dreb": r.get("DREB", 0),
                "plus_minus": r.get("PLUS_MINUS", 0),
            })

        result = {
            "player_id": player_id,
            "player_name": player["full_name"],
            "season": season,
            "games_count": len(games),
            "games": games,
        }

        set_cached(cache_key, result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e), "player": player_name}), 500


@app.route("/api/nba/player/<player_name>/stats")
def nba_player_stats(player_name):
    """
    Returns processed stats for a specific market (points, rebounds, etc.)
    Query params: market (e.g. 'points', 'rebounds'), line (e.g. 23.5)
    Response: { avg5, avg15, variance, hit_prob_over, hit_prob_under, actual_hit_rate, games }
    """
    market = request.args.get("market", "points")
    line = float(request.args.get("line", 0))
    season = request.args.get("season", "2025-26")

    if line <= 0:
        return jsonify({"error": "line parameter required"}), 400

    # Map market to gamelog key
    market_key_map = {
        "points": "pts", "rebounds": "reb", "assists": "ast",
        "threes": "fg3m", "blocks": "blk", "steals": "stl",
        "turnovers": "tov", "totalRebounds": "reb",
        "threePointFieldGoalsMade": "fg3m",
        "fieldGoalsMade": "fgm",
    }
    stat_key = market_key_map.get(market, market)

    # Get gamelog (uses cache)
    cache_key = f"{player_name.lower()}_{season}"
    cached = get_cached(cache_key)
    if not cached:
        # Fetch fresh
        resp = nba_gamelog(player_name)
        if resp.status_code != 200:
            return resp
        cached = resp.get_json()

    games = cached.get("games", [])
    # Filter out DNP (0 minutes)
    games = [g for g in games if g.get("min", 0) and g["min"] != 0]

    if not games:
        return jsonify({"error": "No games found"}), 404

    vals = [g.get(stat_key, 0) for g in games]
    recent5 = vals[:5]  # gamelog is newest-first
    recent15 = vals[:15]

    if not recent5:
        return jsonify({"error": "No recent games"}), 404

    avg5 = sum(recent5) / len(recent5)
    avg15 = sum(recent15) / len(recent15) if len(recent15) >= 5 else avg5

    # Variance
    sample = vals[:max(10, len(recent15))]
    sample_mean = sum(sample) / len(sample)
    variance = sum((v - sample_mean) ** 2 for v in sample) / (len(sample) - 1) if len(sample) > 1 else sample_mean * 1.5

    # Hit rate
    actual_over = sum(1 for v in recent5 if v > line)
    actual_hit_rate = actual_over / len(recent5)

    # Negative binomial probability (simplified)
    import math
    def poisson_cdf(lam, k):
        cdf = 0
        for i in range(int(k) + 1):
            p = math.exp(-lam)
            for j in range(1, i + 1):
                p *= lam / j
            cdf += p
        return min(1, max(0, cdf))

    if variance <= avg5 * 1.01 or avg5 <= 0:
        hit_prob_under = poisson_cdf(avg5, line)
    else:
        r = (avg5 ** 2) / (variance - avg5)
        p = avg5 / variance
        if r <= 0 or p <= 0 or p >= 1:
            hit_prob_under = 0.5
        else:
            cdf = 0
            log_pmf = r * math.log(p)
            cdf += math.exp(log_pmf)
            for k in range(1, int(line) + 1):
                log_pmf += math.log((k + r - 1) / k) + math.log(1 - p)
                cdf += math.exp(log_pmf)
                if cdf > 0.999:
                    break
            hit_prob_under = min(1, max(0, cdf))

    hit_prob_over = 1 - hit_prob_under

    # Latest game date
    latest_date = games[0].get("date") if games else None

    return jsonify({
        "player_name": cached.get("player_name"),
        "market": market,
        "stat_key": stat_key,
        "line": line,
        "avg5": round(avg5, 2),
        "avg15": round(avg15, 2),
        "variance": round(variance, 2),
        "hit_prob_over": round(hit_prob_over, 4),
        "hit_prob_under": round(hit_prob_under, 4),
        "actual_hit_rate": round(actual_hit_rate, 4),
        "games_used": len(recent5),
        "total_games": len(games),
        "latest_date": latest_date,
        "recent_values": recent5,
    })


# ── NHL Endpoints ─────────────────────────────────────────────────────────────
# Proxies api-web.nhle.com to bypass CORS (gamelog endpoint has no CORS headers)

@app.route("/api/nhl/player/<player_name>/gamelog")
def nhl_gamelog(player_name):
    """
    Returns recent gamelog for an NHL player.
    Proxies: search.d3.nhle.com (player search) + api-web.nhle.com (gamelog)
    """
    cache_key = f"nhl_{player_name.lower()}"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        # Search for player by full name
        search_res = http_requests.get(
            f"https://search.d3.nhle.com/api/v1/search/player?culture=en-us&limit=5&q={player_name}",
            timeout=10,
        )
        if search_res.status_code != 200:
            return jsonify({"error": "NHL search failed"}), 502

        players_list = search_res.json()
        p_lower = strip_accents(player_name.lower())

        # Match: strip accents for comparison (Lafrenière vs Lafreniere)
        match = None
        for p in players_list:
            n = strip_accents((p.get("name") or "").lower())
            if n == p_lower:
                match = p
                break
        if not match:
            first = p_lower.split()[0] if " " in p_lower else ""
            last = p_lower.split()[-1]
            for p in players_list:
                n = strip_accents((p.get("name") or "").lower())
                if last in n and first in n:
                    match = p
                    break

        if not match:
            return jsonify({"error": "Player not found", "player": player_name}), 404

        player_id = match["playerId"]

        # Fetch gamelog
        gl_res = http_requests.get(
            f"https://api-web.nhle.com/v1/player/{player_id}/game-log/20252026/2",
            timeout=10,
        )
        if gl_res.status_code != 200:
            return jsonify({"error": "NHL gamelog failed"}), 502

        gl_data = gl_res.json()
        game_log = gl_data.get("gameLog", [])

        games = []
        for g in game_log:
            # Skip DNP
            if g.get("toi") in ("00:00", "0:00", None):
                continue
            games.append({
                "date": g.get("gameDate"),
                "opponent": g.get("opponentAbbrev"),
                "is_home": g.get("homeRoadFlag") == "H",
                "goals": g.get("goals", 0),
                "assists": g.get("assists", 0),
                "points": g.get("points", 0),
                "shots": g.get("shots", 0),
                "pim": g.get("pim", 0),
                "pp_goals": g.get("powerPlayGoals", 0),
                "pp_points": g.get("powerPlayPoints", 0),
                "toi": g.get("toi"),
                "plus_minus": g.get("plusMinus", 0),
            })

        result = {
            "player_id": player_id,
            "player_name": match.get("name"),
            "team": match.get("teamAbbrev"),
            "games_count": len(games),
            "games": games,
        }

        set_cached(cache_key, result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e), "player": player_name}), 500


# ── Soccer Endpoints ──────────────────────────────────────────────────────────
# Fetches team form from TheSportsDB (free, no key) by scraping last 4 rounds.
# Returns goals for/against, W-D-L, home/away splits for Poisson model.

SOCCER_LEAGUES = {
    "epl": {"id": 4328, "season": "2025-2026", "name": "English Premier League"},
    "laliga": {"id": 4335, "season": "2025-2026", "name": "Spanish La Liga"},
    "fa_cup": {"id": 4482, "season": "2025-2026", "name": "FA Cup"},
    "ucl": {"id": 4480, "season": "2025-2026", "name": "UEFA Champions League"},
}


def fetch_league_results(league_id, season, rounds_back=4):
    """Fetch completed match results from recent rounds."""
    all_matches = []
    found_any = False
    misses = 0  # consecutive rounds with no data — stop early if too many

    for r in range(40, 0, -1):
        try:
            res = http_requests.get(
                f"https://www.thesportsdb.com/api/v1/json/3/eventsround.php?id={league_id}&r={r}&s={season}",
                timeout=5,
            )
            if res.status_code != 200:
                misses += 1
                if found_any and misses > 3:
                    break  # stop scanning if we already found data and hit 3 empty rounds
                continue
            events = res.json().get("events") or []
            completed = [
                e for e in events
                if e.get("intHomeScore") is not None and e.get("intHomeScore") != ""
            ]
            if completed:
                all_matches.extend(completed)
                rounds_back -= 1
                found_any = True
                misses = 0
                if rounds_back <= 0:
                    break
            else:
                misses += 1
                if found_any and misses > 3:
                    break
        except Exception:
            misses += 1
            if found_any and misses > 3:
                break
            continue
    return all_matches


def compute_team_form(matches, team_name, max_games=10):
    """Compute W-D-L, goals for/against, home/away from match list."""
    games = []
    for m in matches:
        home = m.get("strHomeTeam", "")
        away = m.get("strAwayTeam", "")
        h_score = int(m.get("intHomeScore") or 0)
        a_score = int(m.get("intAwayScore") or 0)
        date = m.get("dateEvent", "")

        if home.lower() == team_name.lower():
            games.append({"gf": h_score, "ga": a_score, "is_home": True, "date": date})
        elif away.lower() == team_name.lower():
            games.append({"gf": a_score, "ga": h_score, "is_home": False, "date": date})

    # Sort by date descending, take last N
    games.sort(key=lambda x: x["date"], reverse=True)
    games = games[:max_games]

    if not games:
        return None

    wins = sum(1 for g in games if g["gf"] > g["ga"])
    draws = sum(1 for g in games if g["gf"] == g["ga"])
    losses = sum(1 for g in games if g["gf"] < g["ga"])
    gf = sum(g["gf"] for g in games)
    ga = sum(g["ga"] for g in games)
    n = len(games)

    home_games = [g for g in games if g["is_home"]]
    away_games = [g for g in games if not g["is_home"]]

    return {
        "wins": wins, "draws": draws, "losses": losses,
        "goals_for": round(gf / n, 2),
        "goals_against": round(ga / n, 2),
        "games": n,
        "home_goals_for": round(sum(g["gf"] for g in home_games) / len(home_games), 2) if home_games else None,
        "home_goals_ag": round(sum(g["ga"] for g in home_games) / len(home_games), 2) if home_games else None,
        "away_goals_for": round(sum(g["gf"] for g in away_games) / len(away_games), 2) if away_games else None,
        "away_goals_ag": round(sum(g["ga"] for g in away_games) / len(away_games), 2) if away_games else None,
        "home_games": len(home_games),
        "away_games": len(away_games),
        "latest_date": games[0]["date"] if games else None,
    }


@app.route("/api/soccer/team/<team_name>/form")
def soccer_team_form(team_name):
    """
    Returns team form (W-D-L, goals for/against, home/away splits).
    Query params: league (epl, laliga, fa_cup, ucl)
    """
    league_key = request.args.get("league", "epl")
    league = SOCCER_LEAGUES.get(league_key)
    if not league:
        return jsonify({"error": f"Unknown league: {league_key}"}), 400

    cache_key = f"soccer_{league_key}_{team_name.lower()}"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        matches = fetch_league_results(league["id"], league["season"])
        if not matches:
            return jsonify({"error": "No matches found"}), 404

        form = compute_team_form(matches, team_name)
        if not form:
            # Try partial name match
            team_lower = team_name.lower()
            all_teams = set()
            for m in matches:
                all_teams.add(m.get("strHomeTeam", ""))
                all_teams.add(m.get("strAwayTeam", ""))
            match = next((t for t in all_teams if team_lower in t.lower() or t.lower() in team_lower), None)
            if match:
                form = compute_team_form(matches, match)

        if not form:
            return jsonify({"error": "Team not found", "team": team_name}), 404

        result = {
            "team": team_name,
            "league": league["name"],
            **form,
        }
        set_cached(cache_key, result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/soccer/scorers/<league_key>")
def soccer_scorers(league_key):
    """
    Returns top 50 scorers for a league with goals-per-game.
    Uses ESPN statistics endpoint (works from Python, CORS OK but data is better aggregated here).
    League keys: epl, laliga, ucl, fa_cup
    """
    espn_leagues = {
        "epl": "eng.1", "laliga": "esp.1",
        "ucl": "uefa.champions", "fa_cup": "eng.fa",
    }
    espn_id = espn_leagues.get(league_key)
    if not espn_id:
        return jsonify({"error": f"Unknown league: {league_key}"}), 400

    cache_key = f"soccer_scorers_{league_key}"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        res = http_requests.get(
            f"https://site.web.api.espn.com/apis/site/v2/sports/soccer/{espn_id}/statistics",
            timeout=10,
        )
        if res.status_code != 200:
            return jsonify({"error": "ESPN stats failed"}), 502

        data = res.json()
        stats = data.get("stats", [])
        if not stats:
            return jsonify({"error": "No scorer data"}), 404

        goals_cat = stats[0]  # first category is always goals
        leaders = goals_cat.get("leaders", [])

        scorers = []
        for l in leaders:
            ath = l.get("athlete", {})
            display = l.get("displayValue", "")
            # Parse "Matches: 29, Goals: 22"
            parts = {p.split(":")[0].strip(): p.split(":")[1].strip()
                     for p in display.split(",") if ":" in p}
            matches = int(parts.get("Matches", 0))
            goals = int(parts.get("Goals", 0))
            gpg = round(goals / matches, 3) if matches > 0 else 0

            scorers.append({
                "name": ath.get("displayName", ""),
                "team": ath.get("team", {}).get("displayName", ""),
                "id": ath.get("id"),
                "matches": matches,
                "goals": goals,
                "goals_per_game": gpg,
            })

        result = {"league": league_key, "scorers_count": len(scorers), "scorers": scorers}
        set_cached(cache_key, result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Soccer Player Stats (Understat) ──────────────────────────────────────────
# Real xG, shots, xA, key passes for EPL + La Liga (500+ players per league)

UNDERSTAT_LEAGUES = {
    "epl": "EPL",
    "laliga": "La_Liga",
}


@app.route("/api/soccer/players/<league_key>")
def soccer_player_stats(league_key):
    """
    Returns all player stats for a league: xG, shots, xA, goals, assists, key passes.
    League keys: epl, laliga
    """
    us_league = UNDERSTAT_LEAGUES.get(league_key)
    if not us_league:
        return jsonify({"error": f"Understat doesn't cover {league_key}. Only EPL and La Liga."}), 400

    cache_key = f"understat_{league_key}"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        client = UnderstatClient()
        players = client.league(league=us_league).get_player_data(season="2025")

        player_list = []
        for p in players:
            games = int(p.get("games", 0))
            if games < 3:
                continue
            goals = int(p.get("goals", 0))
            shots = int(p.get("shots", 0))
            xg = float(p.get("xG", 0))
            xa = float(p.get("xA", 0))
            key_passes = int(p.get("key_passes", 0))
            assists = int(p.get("assists", 0))
            mins = int(p.get("time", 0))

            player_list.append({
                "name": p.get("player_name", ""),
                "team": p.get("team_title", ""),
                "id": p.get("id"),
                "games": games,
                "goals": goals,
                "shots": shots,
                "xg": round(xg, 2),
                "xa": round(xa, 2),
                "assists": assists,
                "key_passes": key_passes,
                "minutes": mins,
                "goals_per_game": round(goals / games, 3) if games else 0,
                "shots_per_game": round(shots / games, 2) if games else 0,
                "xg_per_game": round(xg / games, 3) if games else 0,
                "xa_per_game": round(xa / games, 3) if games else 0,
            })

        player_list.sort(key=lambda x: x["goals"], reverse=True)

        result = {
            "league": league_key,
            "season": "2025-26",
            "players_count": len(player_list),
            "players": player_list,
        }

        set_cached(cache_key, result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/soccer/player/<player_name>/stats")
def soccer_player_detail(player_name):
    """
    Returns stats for a specific player across available leagues.
    Searches EPL and La Liga.
    """
    cache_key = f"understat_player_{player_name.lower()}"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    p_lower = player_name.lower()

    for league_key, us_league in UNDERSTAT_LEAGUES.items():
        try:
            client = UnderstatClient()
            players = client.league(league=us_league).get_player_data(season="2025")
            match = None
            for p in players:
                n = (p.get("player_name") or "").lower()
                if n == p_lower or (p_lower.split()[-1] in n and p_lower.split()[0] in n):
                    match = p
                    break

            if match:
                games = int(match.get("games", 0))
                result = {
                    "name": match.get("player_name"),
                    "team": match.get("team_title"),
                    "league": league_key,
                    "games": games,
                    "goals": int(match.get("goals", 0)),
                    "shots": int(match.get("shots", 0)),
                    "xg": round(float(match.get("xG", 0)), 2),
                    "xa": round(float(match.get("xA", 0)), 2),
                    "assists": int(match.get("assists", 0)),
                    "key_passes": int(match.get("key_passes", 0)),
                    "minutes": int(match.get("time", 0)),
                    "goals_per_game": round(int(match.get("goals", 0)) / games, 3) if games else 0,
                    "shots_per_game": round(int(match.get("shots", 0)) / games, 2) if games else 0,
                    "xg_per_game": round(float(match.get("xG", 0)) / games, 3) if games else 0,
                }
                set_cached(cache_key, result)
                return jsonify(result)

        except Exception:
            continue

    return jsonify({"error": "Player not found", "player": player_name}), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
