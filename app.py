"""
NBA Stats Proxy — serves real-time NBA player gamelogs to the betting dashboard.
Uses nba_api to bypass NBA.com's bot protection (works from Python, blocked from browser).
Deploy to Render free tier. Dashboard calls this instead of ESPN for NBA stats.
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import players
import time

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
