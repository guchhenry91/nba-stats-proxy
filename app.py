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
import re as _re


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


# ── Statmuse NBA Game Log (real-time last-5) ──────────────────────────────────
# Bypasses nba_api / NBA.com staleness by fetching directly from Statmuse SSR.
# Statmuse serves real stat tables in server-rendered HTML — no JS needed.

STATMUSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.statmuse.com/",
}

# Slug map — Statmuse uses player-name-ID format
STATMUSE_SLUGS = {
    "scottie barnes":            "scottie-barnes-30239",
    "bam adebayo":               "bam-adebayo-4066",
    "jalen brunson":             "jalen-brunson-9795",
    "jayson tatum":              "jayson-tatum-9582",
    "tyrese maxey":              "tyrese-maxey-30192",
    "alperen sengun":            "alperen-sengun-30314",
    "lebron james":              "lebron-james-1780",
    "kevin durant":              "kevin-durant-985",
    "pascal siakam":             "pascal-siakam-3059",
    "nikola jokic":              "nikola-jokic-9226",
    "shai gilgeous-alexander":   "shai-gilgeous-alexander-9773",
    "jaylen brown":              "jaylen-brown-9390",
    "anthony edwards":           "anthony-edwards-30191",
    "luka doncic":               "luka-doncic-9988",
    "victor wembanyama":         "victor-wembanyama-30249",
    "cade cunningham":           "cade-cunningham-30241",
    "donovan mitchell":          "donovan-mitchell-9695",
    "karl-anthony towns":        "karl-anthony-towns-3059",
    "devin booker":              "devin-booker-9502",
    "trae young":                "trae-young-9988",
    "giannis antetokounmpo":     "giannis-antetokounmpo-3032",
    "stephen curry":             "stephen-curry-791",
    "joel embiid":               "joel-embiid-3157",
    "damian lillard":            "damian-lillard-2571",
    "jimmy butler":              "jimmy-butler-2619",
    "zion williamson":           "zion-williamson-30099",
    "ja morant":                 "ja-morant-30107",
    "darius garland":            "darius-garland-30101",
    "evan mobley":               "evan-mobley-30237",
    "jarrett allen":             "jarrett-allen-9611",
    "paul george":               "paul-george-1621",
    "kawhi leonard":             "kawhi-leonard-2001",
    "james harden":              "james-harden-686",
    "kyrie irving":              "kyrie-irving-1677",
    "russell westbrook":         "russell-westbrook-706",
    "chris paul":                "chris-paul-383",
    "lamelo ball":               "lamelo-ball-30199",
    "miles bridges":             "miles-bridges-9711",
    "brandon miller":            "brandon-miller-30250",
    "paolo banchero":            "paolo-banchero-30244",
    "franz wagner":              "franz-wagner-30233",
    "jalen suggs":               "jalen-suggs-30238",
    "dejounte murray":           "dejounte-murray-4061",
    "trae young":                "trae-young-9988",
    "onyeka okongwu":            "onyeka-okongwu-30195",
    "clint capela":              "clint-capela-3074",
    "nikola vucevic":            "nikola-vucevic-2011",
    "domantas sabonis":          "domantas-sabonis-3940",
    "de'aaron fox":              "deaaron-fox-9575",
    "keegan murray":             "keegan-murray-30243",
    "andrew wiggins":            "andrew-wiggins-3073",
    "draymond green":            "draymond-green-2561",
    "klay thompson":             "klay-thompson-1470",
    "bam adebayo":               "bam-adebayo-4066",
    "tyler herro":               "tyler-herro-30100",
    "terry rozier":              "terry-rozier-3938",
    "jrue holiday":              "jrue-holiday-1424",
    "kristaps porzingis":        "kristaps-porzingis-3898",
    "al horford":                "al-horford-370",
    "derrick white":             "derrick-white-9578",
    "payton pritchard":          "payton-pritchard-30181",
    "julius randle":             "julius-randle-3151",
    "og anunoby":                "og-anunoby-9699",
    "mikal bridges":             "mikal-bridges-9713",
    "josh hart":                 "josh-hart-9576",
    "donte divincenzo":          "donte-divincenzo-9714",
    "zach lavine":               "zach-lavine-3147",
    "coby white":                "coby-white-30102",
    "nikola jokic":              "nikola-jokic-9226",
    "michael porter jr":         "michael-porter-jr-9741",
    "jamal murray":              "jamal-murray-9412",
    "aaron gordon":              "aaron-gordon-3150",
    "anfernee simons":           "anfernee-simons-9748",
    "scoot henderson":           "scoot-henderson-30251",
    "shaedon sharpe":            "shaedon-sharpe-30245",
    "jerami grant":              "jerami-grant-3153",
    "fred vanvleet":             "fred-vanvleet-9413",
    "immanuel quickley":         "immanuel-quickley-30190",
    "rj barrett":                "rj-barrett-30103",
    "cam thomas":                "cam-thomas-30240",
    "ben simmons":               "ben-simmons-4052",
    "dennis schroder":           "dennis-schroder-2692",
    "austin reaves":             "austin-reaves-30223",
    "d'angelo russell":          "dangelo-russell-3886",
    "anthony davis":             "anthony-davis-2440",
}


def parse_statmuse_gamelog(html_text):
    """
    Parse Statmuse SSR game log page.
    Stats are embedded in the rendered HTML as text in table cells.
    Returns dict of lists: pts, reb, ast, stl, blk (newest-first)
    """
    games = {"pts": [], "reb": [], "ast": [], "stl": [], "blk": []}

    # Extract summary line (e.g. "averaged 12.8 points, 8.2 assists...")
    summary = ""
    summary_match = _re.search(r"averaged (.+?) in \d+ games", html_text)
    if summary_match:
        summary = summary_match.group(0)

    # Extract stat rows from HTML table — look for <td> values in game log table
    # Each row contains: G, Date, Opponent, Result, MIN, PTS, REB, AST, STL, BLK, ...
    row_pattern = _re.compile(
        r'<tr[^>]*>.*?</tr>', _re.DOTALL
    )
    cell_pattern = _re.compile(r'<td[^>]*>(.*?)</td>', _re.DOTALL)
    tag_strip = _re.compile(r'<[^>]+>')

    for row_match in row_pattern.finditer(html_text):
        row = row_match.group(0)
        cells = [tag_strip.sub('', c).strip() for c in cell_pattern.findall(row)]
        # Need at least 10 columns: G Date Opp Result MIN PTS REB AST STL BLK
        if len(cells) < 10:
            continue
        # First cell should be a game number
        try:
            int(cells[0])
        except (ValueError, IndexError):
            continue
        # MIN is cells[4], validate it's a plausible minutes value
        try:
            mins = float(cells[4])
            if not (0 < mins <= 50):
                continue
            pts  = float(cells[5])
            reb  = float(cells[6])
            ast  = float(cells[7])
            stl  = float(cells[8])
            blk  = float(cells[9])
        except (ValueError, IndexError):
            continue

        if 0 <= pts <= 80:  games["pts"].append(pts)
        if 0 <= reb <= 30:  games["reb"].append(reb)
        if 0 <= ast <= 25:  games["ast"].append(ast)
        if 0 <= stl <= 10:  games["stl"].append(stl)
        if 0 <= blk <= 10:  games["blk"].append(blk)

    games["summary"] = summary
    return games


@app.route("/api/nba/player/<player_name>/gamelog/statmuse")
def nba_gamelog_statmuse(player_name):
    """
    Returns real-time last-N game log from Statmuse SSR.
    Fresher than nba_api — updates within hours of game end.
    Response mirrors /gamelog format so frontend swaps sources transparently.
    Query params: games (default 5, max 10)
    """
    n_games = min(int(request.args.get("games", 5)), 10)
    cache_key = f"statmuse_{player_name.lower()}"

    # 30-minute TTL — much shorter than nba_api's 2h so stats stay fresh
    if cache_key in _cache:
        ts, data = _cache[cache_key]
        if time.time() - ts < 1800:
            return jsonify(data)

    # Slug lookup — exact then partial
    slug = STATMUSE_SLUGS.get(player_name.lower())
    if not slug:
        p_lower = player_name.lower()
        for key, val in STATMUSE_SLUGS.items():
            if p_lower in key or key in p_lower:
                slug = val
                break

    if not slug:
        return jsonify({
            "error": "Player slug not found. Add to STATMUSE_SLUGS in app.py.",
            "player": player_name,
            "available": sorted(STATMUSE_SLUGS.keys()),
        }), 404

    try:
        url = f"https://www.statmuse.com/nba/player/{slug}/game-log?seasonYear=2026"
        resp = http_requests.get(url, headers=STATMUSE_HEADERS, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": f"Statmuse returned {resp.status_code}"}), 502

        games_data = parse_statmuse_gamelog(resp.text)

        pts_list = games_data["pts"][:n_games]
        reb_list = games_data["reb"][:n_games]
        ast_list = games_data["ast"][:n_games]
        stl_list = games_data["stl"][:n_games]
        blk_list = games_data["blk"][:n_games]

        n = len(pts_list)
        if n == 0:
            return jsonify({"error": "No game data parsed from Statmuse page"}), 500

        games = []
        for i in range(n):
            games.append({
                "pts": pts_list[i] if i < len(pts_list) else 0,
                "reb": reb_list[i] if i < len(reb_list) else 0,
                "ast": ast_list[i] if i < len(ast_list) else 0,
                "stl": stl_list[i] if i < len(stl_list) else 0,
                "blk": blk_list[i] if i < len(blk_list) else 0,
                "min": None,
                "is_home": None,
            })

        avgs = {
            "avg_pts": round(sum(pts_list) / len(pts_list), 1) if pts_list else 0,
            "avg_reb": round(sum(reb_list) / len(reb_list), 1) if reb_list else 0,
            "avg_ast": round(sum(ast_list) / len(ast_list), 1) if ast_list else 0,
        }

        result = {
            "player_name": player_name,
            "source": "statmuse",
            "games_count": n,
            "summary": games_data.get("summary", ""),
            "averages": avgs,
            "games": games,
        }

        _cache[cache_key] = (time.time(), result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e), "player": player_name}), 500


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


# ── Soccer Team xG (Understat) ───────────────────────────────────────────────
# Independent team-level xG/xGA — the foundation for breaking odds dependency.
# Covers: EPL, La Liga, Bundesliga, Serie A, Ligue 1

UNDERSTAT_TEAM_LEAGUES = {
    "epl": "EPL",
    "laliga": "La_Liga",
    "bundesliga": "Bundesliga",
    "seriea": "Serie_A",
    "ligue1": "Ligue_1",
}


@app.route("/api/soccer/team-xg/<league_key>")
def soccer_team_xg(league_key):
    """
    Returns all teams' xG stats for a league — independent of bookmaker odds.
    Each team gets: xG/game, xGA/game, npxG, npxGA, goals scored/conceded,
    home and away splits, recent form (last 5 and last 10).
    """
    us_league = UNDERSTAT_TEAM_LEAGUES.get(league_key)
    if not us_league:
        return jsonify({"error": f"Understat doesn't cover {league_key}. Available: {list(UNDERSTAT_TEAM_LEAGUES.keys())}"}), 400

    cache_key = f"team_xg_{league_key}"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        with UnderstatClient() as client:
            teams_data = client.league(league=us_league).get_team_data(season="2024")

        teams = []
        # Handle both dict format (old) and list format (new)
        if isinstance(teams_data, list):
            items = [(t.get("title") or t.get("team_name", f"Team_{i}"), t) for i, t in enumerate(teams_data)]
        elif isinstance(teams_data, dict):
            items = list(teams_data.items())
        else:
            return jsonify({"error": f"Unexpected data format: {type(teams_data).__name__}"}), 500

        for team_name, team_info in items:
            history = team_info.get("history", [])
            if not history:
                continue

            # Full season aggregates
            total_xg = sum(float(m.get("xG", 0)) for m in history)
            total_xga = sum(float(m.get("xGA", 0)) for m in history)
            total_npxg = sum(float(m.get("npxG", 0)) for m in history)
            total_npxga = sum(float(m.get("npxGA", 0)) for m in history)
            total_scored = sum(int(m.get("scored", 0)) for m in history)
            total_missed = sum(int(m.get("missed", 0)) for m in history)
            n = len(history)

            # Home/away splits
            home_matches = [m for m in history if m.get("h_a") == "h"]
            away_matches = [m for m in history if m.get("h_a") == "a"]

            def avg_xg(matches):
                if not matches:
                    return None
                return round(sum(float(m.get("xG", 0)) for m in matches) / len(matches), 3)

            def avg_xga(matches):
                if not matches:
                    return None
                return round(sum(float(m.get("xGA", 0)) for m in matches) / len(matches), 3)

            # Recent form: last 5 and last 10 matches (sorted by date, newest last in history)
            recent5 = history[-5:] if len(history) >= 5 else history
            recent10 = history[-10:] if len(history) >= 10 else history

            teams.append({
                "team": team_name,
                "games": n,
                "xg_total": round(total_xg, 2),
                "xga_total": round(total_xga, 2),
                "xg_per_game": round(total_xg / n, 3),
                "xga_per_game": round(total_xga / n, 3),
                "npxg_per_game": round(total_npxg / n, 3),
                "npxga_per_game": round(total_npxga / n, 3),
                "goals_scored": total_scored,
                "goals_conceded": total_missed,
                "goals_per_game": round(total_scored / n, 3),
                "goals_conceded_per_game": round(total_missed / n, 3),
                # Home/away xG splits
                "home_xg_per_game": avg_xg(home_matches),
                "home_xga_per_game": avg_xga(home_matches),
                "away_xg_per_game": avg_xg(away_matches),
                "away_xga_per_game": avg_xga(away_matches),
                "home_games": len(home_matches),
                "away_games": len(away_matches),
                # Recent form xG
                "recent5_xg": round(sum(float(m.get("xG", 0)) for m in recent5) / len(recent5), 3),
                "recent5_xga": round(sum(float(m.get("xGA", 0)) for m in recent5) / len(recent5), 3),
                "recent10_xg": round(sum(float(m.get("xG", 0)) for m in recent10) / len(recent10), 3),
                "recent10_xga": round(sum(float(m.get("xGA", 0)) for m in recent10) / len(recent10), 3),
            })

        # Sort by xG per game descending
        teams.sort(key=lambda t: t["xg_per_game"], reverse=True)

        # Compute league averages (needed for attack/defense strength calculation)
        all_xg = [t["xg_per_game"] for t in teams]
        all_xga = [t["xga_per_game"] for t in teams]
        league_avg_xg = round(sum(all_xg) / len(all_xg), 3) if all_xg else 1.3
        league_avg_xga = round(sum(all_xga) / len(all_xga), 3) if all_xga else 1.3

        result = {
            "league": league_key,
            "season": "2025-26",
            "teams_count": len(teams),
            "league_avg_xg": league_avg_xg,
            "league_avg_xga": league_avg_xga,
            "teams": teams,
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
        with UnderstatClient() as client:
            players = client.league(league=us_league).get_player_data(season="2024")

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
