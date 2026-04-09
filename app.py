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


# ── Statmuse Game Logs (NBA / NHL / MLB) — real-time SSR scraping ─────────────
# Statmuse serves fully rendered HTML with stat tables at 200 from raw requests.
# Confirmed working: NBA ✅  NHL ✅  MLB ✅  (tested locally Apr 2026)
# Row format (confirmed from live HTML):
#   NBA: [Date, Team, vs/@, Opp, Result, MIN, PTS, REB, AST, STL, BLK, FG3M, ...]
#   NHL: [Date, Team, vs/@, Opp, Result, G,   A,   PTS, +/-, SOG, PIM, TOI,  ...]
#   MLB: [Date, Team, vs/@, Opp, Result, AB,  R,   H,   HR,  RBI, BB,  K,    ...]

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


_SM_ROW = _re.compile(r'<tr[^>]*>(.*?)</tr>', _re.DOTALL)
_SM_CELL = _re.compile(r'<td[^>]*>(.*?)</td>', _re.DOTALL)
_SM_TAG = _re.compile(r'<[^>]+>')

def _sm_parse_rows(html_text):
    """
    Parse all <tr> rows from Statmuse HTML, return list of cell-value lists.
    Skips header rows, Average rows, and rows with fewer than 8 cells.
    Confirmed row format for all sports:
      cells[0] = Date  (e.g. "Sun 4/9")
      cells[1] = Team  (e.g. "WAS")
      cells[2] = vs/@
      cells[3] = Opp
      cells[4] = Result (e.g. "W 114-108")
      cells[5..] = stats (sport-specific)
    """
    rows = []
    for m in _SM_ROW.finditer(html_text):
        cells = [_SM_TAG.sub('', c).strip() for c in _SM_CELL.findall(m.group(1))]
        cells = [c for c in cells if c]
        if len(cells) < 8:
            continue
        # Skip header rows and Average/Total rows
        if cells[0] in ('Average', 'Total', 'G', 'Date', '#'):
            continue
        # First cell must look like a date: "Mon 4/7", "Sat 3/22" etc.
        if not _re.match(r'^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+\d', cells[0]):
            continue
        # cells[2] must be "vs" or "@"
        if cells[2] not in ('vs', '@'):
            continue
        rows.append(cells)
    return rows


def _safe_float(val, lo, hi):
    try:
        v = float(val)
        return v if lo <= v <= hi else None
    except (ValueError, TypeError):
        return None


@app.route("/api/nba/player/<player_name>/gamelog/statmuse")
def nba_gamelog_statmuse(player_name):
    """
    Real-time NBA game log from Statmuse SSR (30-min cache).
    Row format: [Date, Team, vs/@, Opp, Result, MIN, PTS, REB, AST, STL, BLK, FG3M, ...]
    Falls back: frontend uses /gamelog (nba_api) if 404 returned.
    """
    n_games = min(int(request.args.get("games", 10)), 20)
    cache_key = f"statmuse_nba_{player_name.lower()}"
    if cache_key in _cache:
        ts, data = _cache[cache_key]
        if time.time() - ts < 1800:
            return jsonify(data)

    slug = STATMUSE_SLUGS.get(player_name.lower())
    if not slug:
        p_lower = player_name.lower()
        for key, val in STATMUSE_SLUGS.items():
            if p_lower in key or key in p_lower:
                slug = val
                break
    if not slug:
        return jsonify({"error": "slug_not_found", "player": player_name}), 404

    try:
        resp = http_requests.get(
            f"https://www.statmuse.com/nba/player/{slug}/game-log?seasonYear=2026",
            headers=STATMUSE_HEADERS, timeout=15
        )
        if resp.status_code != 200:
            return jsonify({"error": f"Statmuse {resp.status_code}"}), 502

        games = []
        for cells in _sm_parse_rows(resp.text):
            # cells[5]=MIN, [6]=PTS, [7]=REB, [8]=AST, [9]=STL, [10]=BLK, [11]=FG3M
            mins = _safe_float(cells[5], 0, 50)
            if mins is None or mins == 0:
                continue  # skip DNP
            games.append({
                "date":     cells[0],
                "is_home":  cells[2] == "vs",
                "min":      mins,
                "pts":      _safe_float(cells[6],  0, 80) or 0,
                "reb":      _safe_float(cells[7],  0, 30) or 0,
                "ast":      _safe_float(cells[8],  0, 25) or 0,
                "stl":      _safe_float(cells[9],  0, 10) or 0,
                "blk":      _safe_float(cells[10], 0, 10) or 0,
                "fg3m":     _safe_float(cells[11], 0, 15) or 0 if len(cells) > 11 else 0,
            })
            if len(games) >= n_games:
                break

        if not games:
            return jsonify({"error": "No rows parsed", "player": player_name}), 500

        result = {
            "player_name": player_name, "source": "statmuse",
            "games_count": len(games), "games": games,
        }
        _cache[cache_key] = (time.time(), result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e), "player": player_name}), 500


# ── NHL slug map ──────────────────────────────────────────────────────────────
STATMUSE_NHL_SLUGS = {
    "nathan mackinnon":      "nathan-mackinnon-1890",
    "leon draisaitl":        "leon-draisaitl-3049",
    "connor mcdavid":        "connor-mcdavid-3114",
    "matthew tkachuk":       "matthew-tkachuk-3936",
    "david pastrnak":        "david-pastrnak-2755",
    "auston matthews":       "auston-matthews-3977",
    "mitch marner":          "mitch-marner-3979",
    "mikko rantanen":        "mikko-rantanen-3975",
    "kirill kaprizov":       "kirill-kaprizov-30143",
    "cale makar":            "cale-makar-30083",
    "roman josi":            "roman-josi-1940",
    "victor hedman":         "victor-hedman-1522",
    "brayden point":         "brayden-point-3944",
    "nikita kucherov":       "nikita-kucherov-2519",
    "andrei vasilevskiy":    "andrei-vasilevskiy-2763",
    "sam reinhart":          "sam-reinhart-2843",
    "jake guentzel":         "jake-guentzel-3105",
    "mark scheifele":        "mark-scheifele-2067",
    "kyle connor":           "kyle-connor-4011",
    "sean monahan":          "sean-monahan-2625",
    "elias pettersson":      "elias-pettersson-30059",
    "j.t. miller":           "jt-miller-2605",
    "brady tkachuk":         "brady-tkachuk-30080",
    "tim stutzle":           "tim-stutzle-30197",
    "josh norris":           "josh-norris-30133",
    "claude giroux":         "claude-giroux-1264",
    "trevor zegras":         "trevor-zegras-30179",
    "mason mctavish":        "mason-mctavish-30248",
    "troy terry":            "troy-terry-30044",
    "frank vatrano":         "frank-vatrano-3937",
    "tage thompson":         "tage-thompson-30050",
    "rasmus dahlin":         "rasmus-dahlin-30069",
    "quinn hughes":          "quinn-hughes-30099",
    "adam fox":              "adam-fox-30105",
    "miro heiskanen":        "miro-heiskanen-30064",
    "jonathan huberdeau":    "jonathan-huberdeau-2427",
    "aleksander barkov":     "aleksander-barkov-2806",
    "jack eichel":           "jack-eichel-3001",
    "mark stone":            "mark-stone-2218",
    "anze kopitar":          "anze-kopitar-643",
    "drew doughty":          "drew-doughty-1156",
    "gabriel landeskog":     "gabriel-landeskog-1936",
    "evgeni malkin":         "evgeni-malkin-521",
    "sidney crosby":         "sidney-crosby-435",
    "alex ovechkin":         "alex-ovechkin-418",
}


@app.route("/api/nhl/player/<player_name>/gamelog/statmuse")
def nhl_gamelog_statmuse(player_name):
    """
    Real-time NHL game log from Statmuse SSR (30-min cache).
    Row format: [Date, Team, vs/@, Opp, Result, G, A, PTS, +/-, SOG, PIM, TOI, ...]
    """
    n_games = min(int(request.args.get("games", 10)), 20)
    cache_key = f"statmuse_nhl_{player_name.lower()}"
    if cache_key in _cache:
        ts, data = _cache[cache_key]
        if time.time() - ts < 1800:
            return jsonify(data)

    slug = STATMUSE_NHL_SLUGS.get(player_name.lower())
    if not slug:
        p_lower = player_name.lower()
        for key, val in STATMUSE_NHL_SLUGS.items():
            if p_lower in key or key in p_lower:
                slug = val
                break
    if not slug:
        return jsonify({"error": "slug_not_found", "player": player_name}), 404

    try:
        resp = http_requests.get(
            f"https://www.statmuse.com/nhl/player/{slug}/game-log?seasonYear=2026",
            headers=STATMUSE_HEADERS, timeout=15
        )
        if resp.status_code != 200:
            return jsonify({"error": f"Statmuse {resp.status_code}"}), 502

        games = []
        for cells in _sm_parse_rows(resp.text):
            # cells[5]=G, [6]=A, [7]=PTS, [8]=+/-, [9]=SOG, [10]=PIM, [11]=TOI
            games.append({
                "date":     cells[0],
                "is_home":  cells[2] == "vs",
                "goals":    _safe_float(cells[5],  0, 10) or 0,
                "assists":  _safe_float(cells[6],  0, 10) or 0,
                "points":   _safe_float(cells[7],  0, 10) or 0,
                "shots":    _safe_float(cells[9],  0, 20) or 0,
                "pim":      _safe_float(cells[10], 0, 50) or 0 if len(cells) > 10 else 0,
                "toi":      cells[11] if len(cells) > 11 else None,
                "blocked_shots": 0,  # not in Statmuse table
                "pp_goals": 0,
                "pp_points": 0,
            })
            if len(games) >= n_games:
                break

        if not games:
            return jsonify({"error": "No rows parsed", "player": player_name}), 500

        result = {
            "player_name": player_name, "source": "statmuse",
            "games_count": len(games), "games": games,
        }
        _cache[cache_key] = (time.time(), result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e), "player": player_name}), 500


# ── MLB slug map ──────────────────────────────────────────────────────────────
STATMUSE_MLB_SLUGS = {
    "shohei ohtani":         "shohei-ohtani-12592",
    "freddie freeman":       "freddie-freeman-6418",
    "mookie betts":          "mookie-betts-7578",
    "juan soto":             "juan-soto-13865",
    "aaron judge":           "aaron-judge-10309",
    "pete alonso":           "pete-alonso-14069",
    "bryce harper":          "bryce-harper-5680",
    "trea turner":           "trea-turner-9886",
    "austin riley":          "austin-riley-14144",
    "yordan alvarez":        "yordan-alvarez-14302",
    "jose abreu":            "jose-abreu-6929",
    "mike trout":            "mike-trout-5765",
    "paul goldschmidt":      "paul-goldschmidt-5685",
    "nolan arenado":         "nolan-arenado-5790",
    "ronald acuna jr":       "ronald-acuna-jr-13907",
    "cody bellinger":        "cody-bellinger-10265",
    "corey seager":          "corey-seager-9874",
    "marcus semien":         "marcus-semien-6590",
    "bo bichette":           "bo-bichette-14318",
    "vladimir guerrero jr":  "vladimir-guerrero-jr-14049",
    "jose ramirez":          "jose-ramirez-8077",
    "bobby witt jr":         "bobby-witt-jr-15540",
    "julio rodriguez":       "julio-rodriguez-16225",
    "gunnar henderson":      "gunnar-henderson-16523",
    "corbin carroll":        "corbin-carroll-16278",
    "jackson holliday":      "jackson-holliday-16857",
    "adley rutschman":       "adley-rutschman-15818",
    "will smith":            "will-smith-13911",
    "kyle tucker":           "kyle-tucker-12577",
    "yordan alvarez":        "yordan-alvarez-14302",
    "gerrit cole":           "gerrit-cole-7569",
    "zack wheeler":          "zack-wheeler-7000",
    "sandy alcantara":       "sandy-alcantara-13280",
    "spencer strider":       "spencer-strider-16071",
    "corbin burnes":         "corbin-burnes-13870",
    "max scherzer":          "max-scherzer-5234",
    "justin verlander":      "justin-verlander-3388",
    "jacob degrom":          "jacob-degrom-8210",
    "blake snell":           "blake-snell-10145",
    "yoshinobu yamamoto":    "yoshinobu-yamamoto-17494",
    "paul skenes":           "paul-skenes-17662",
}


@app.route("/api/mlb/player/<player_name>/gamelog/statmuse")
def mlb_gamelog_statmuse(player_name):
    """
    Real-time MLB game log from Statmuse SSR (30-min cache).
    Row format: [Date, Team, vs/@, Opp, Result, AB, R, H, HR, RBI, BB, K, ...]
    """
    n_games = min(int(request.args.get("games", 10)), 20)
    cache_key = f"statmuse_mlb_{player_name.lower()}"
    if cache_key in _cache:
        ts, data = _cache[cache_key]
        if time.time() - ts < 1800:
            return jsonify(data)

    slug = STATMUSE_MLB_SLUGS.get(player_name.lower())
    if not slug:
        p_lower = player_name.lower()
        for key, val in STATMUSE_MLB_SLUGS.items():
            if p_lower in key or key in p_lower:
                slug = val
                break
    if not slug:
        return jsonify({"error": "slug_not_found", "player": player_name}), 404

    try:
        resp = http_requests.get(
            f"https://www.statmuse.com/mlb/player/{slug}/game-log?seasonYear=2025",
            headers=STATMUSE_HEADERS, timeout=15
        )
        if resp.status_code != 200:
            return jsonify({"error": f"Statmuse {resp.status_code}"}), 502

        games = []
        for cells in _sm_parse_rows(resp.text):
            # cells[5]=AB, [6]=R, [7]=H, [8]=HR, [9]=RBI, [10]=BB, [11]=K
            ab = _safe_float(cells[5], 0, 10)
            if ab is None:
                continue
            hits  = _safe_float(cells[7],  0, 7) or 0
            hr    = _safe_float(cells[8],  0, 5) or 0
            rbi   = _safe_float(cells[9],  0, 15) or 0
            bb    = _safe_float(cells[10], 0, 6) or 0
            k     = _safe_float(cells[11], 0, 6) or 0 if len(cells) > 11 else 0
            games.append({
                "date":     cells[0],
                "is_home":  cells[2] == "vs",
                "ab":       ab or 0,
                "runs":     _safe_float(cells[6], 0, 6) or 0,
                "hits":     hits,
                "homeRuns": hr,
                "RBIs":     rbi,
                "walks":    bb,
                "strikeouts": k,
                # Derived composites
                "totalBases":    hits + hr * 3,  # approximate (no 2B/3B from Statmuse)
                "hitsRunsRBIs":  hits + (_safe_float(cells[6], 0, 6) or 0) + rbi,
            })
            if len(games) >= n_games:
                break

        if not games:
            return jsonify({"error": "No rows parsed", "player": player_name}), 500

        result = {
            "player_name": player_name, "source": "statmuse",
            "games_count": len(games), "games": games,
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


# ── Statmuse FC — Soccer Player Game Log ─────────────────────────────────────
# statmuse.com/fc uses /ask/{name-slug}-game-log-{season} — NO numeric ID needed.
# Confirmed working for EPL, La Liga, UCL, Bundesliga, Serie A, Ligue 1 players.
# Row format (confirmed from live HTML):
#   [#, FullName, Date, Team, vs/@, Opp, Rating, MIN, G, A, xG, xA, PK, FK, SH, SOT, ...]
#   cells[0]=# cells[2]=Date cells[3]=Team cells[4]=vs/@ cells[5]=Opp
#   cells[6]=Rating cells[7]=MIN cells[8]=G cells[9]=A cells[10]=xG cells[11]=xA
#   cells[14]=SH (total shots) cells[15]=SOT (shots on target)

def _sm_soccer_name_to_slug(player_name):
    """Convert 'Mohamed Salah' → 'mohamed-salah' for Statmuse FC URL."""
    import unicodedata
    # Strip accents: Mbappé → Mbappe, Müller → Muller
    s = unicodedata.normalize('NFD', player_name)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    # Lowercase, replace spaces/special chars with hyphens
    s = s.lower().strip()
    s = _re.sub(r"['\.]", '', s)   # remove apostrophes and dots
    s = _re.sub(r'[^a-z0-9]+', '-', s)
    s = s.strip('-')
    return s


@app.route("/api/soccer/player/<player_name>/gamelog/statmuse")
def soccer_gamelog_statmuse(player_name):
    """
    Real-time soccer player game log from Statmuse FC (30-min cache).
    Works for any player across EPL, La Liga, UCL, Bundesliga, Serie A, Ligue 1.
    No slug map needed — name is converted directly to URL slug.

    Response fields per game:
      date, is_home, minutes, goals, assists, xg, xa, shots, shots_on_target
    """
    n_games = min(int(request.args.get("games", 10)), 20)
    cache_key = f"statmuse_soccer_{player_name.lower()}"

    if cache_key in _cache:
        ts, data = _cache[cache_key]
        if time.time() - ts < 1800:
            return jsonify(data)

    slug = _sm_soccer_name_to_slug(player_name)

    try:
        # Season 2025-26 → seasonYear param in URL text = "2025-26"
        url = f"https://www.statmuse.com/fc/ask/{slug}-game-log-2025-26"
        resp = http_requests.get(url, headers=STATMUSE_HEADERS, timeout=15)

        # If that 404s try without season (gets current season)
        if resp.status_code == 404:
            url = f"https://www.statmuse.com/fc/ask/{slug}-game-log"
            resp = http_requests.get(url, headers=STATMUSE_HEADERS, timeout=15)

        if resp.status_code != 200:
            return jsonify({"error": f"Statmuse FC {resp.status_code}", "player": player_name, "slug": slug}), 404

        # Parse rows — soccer uses #/Name/Date format (cells[0] = game number)
        row_pattern = _re.compile(r'<tr[^>]*>(.*?)</tr>', _re.DOTALL)
        cell_pattern = _re.compile(r'<td[^>]*>(.*?)</td>', _re.DOTALL)
        tag_strip = _re.compile(r'<[^>]+>')

        games = []
        for m in row_pattern.finditer(resp.text):
            cells = [tag_strip.sub('', c).strip() for c in cell_pattern.findall(m.group(1))]
            cells = [c for c in cells if c]

            # Soccer rows start with a game number (1, 2, 3...)
            if len(cells) < 10:
                continue
            if not _re.match(r'^\d+$', cells[0]):
                continue
            # cells[4] must be vs or @
            if cells[4] not in ('vs', '@'):
                continue

            mins = _safe_float(cells[7], 0, 120)
            if mins is None or mins == 0:
                continue  # skip DNP / red-carded off immediately

            goals  = _safe_float(cells[8],  0, 10) or 0
            assists = _safe_float(cells[9], 0, 10) or 0
            xg     = _safe_float(cells[10], 0, 5)  or 0.0
            xa     = _safe_float(cells[11], 0, 5)  or 0.0
            shots  = _safe_float(cells[14], 0, 20) or 0 if len(cells) > 14 else 0
            sot    = _safe_float(cells[15], 0, 15) or 0 if len(cells) > 15 else 0

            games.append({
                "date":            cells[2],
                "team":            cells[3],
                "is_home":         cells[4] == "vs",
                "opponent":        cells[5],
                "minutes":         mins,
                "goals":           goals,
                "assists":         assists,
                "xg":              xg,
                "xa":              xa,
                "shots":           shots,
                "shots_on_target": sot,
            })
            if len(games) >= n_games:
                break

        if not games:
            return jsonify({
                "error": "No rows parsed — player may not be on Statmuse FC",
                "player": player_name,
                "slug": slug,
                "url_tried": url,
            }), 404

        # Compute season averages
        n = len(games)
        result = {
            "player_name": player_name,
            "source": "statmuse_fc",
            "games_count": n,
            "season": "2025-26",
            "averages": {
                "goals_per_game":   round(sum(g["goals"]   for g in games) / n, 3),
                "assists_per_game": round(sum(g["assists"]  for g in games) / n, 3),
                "xg_per_game":      round(sum(g["xg"]       for g in games) / n, 3),
                "shots_per_game":   round(sum(g["shots"]    for g in games) / n, 2),
                "sot_per_game":     round(sum(g["shots_on_target"] for g in games) / n, 2),
            },
            "games": games,
        }

        _cache[cache_key] = (time.time(), result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e), "player": player_name}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
