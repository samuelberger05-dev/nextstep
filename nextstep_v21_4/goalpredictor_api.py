"""Quota-conscious GoalPredictor adapter for NextStep.
Free API-Football mode: evening-only, cached, max 3 prioritized live matches.
Never put the API key in source control; configure APIFOOTBALL_KEY on the host.
"""
import os, json, urllib.request, urllib.parse, threading, time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

API_BASE = "https://v3.football.api-sports.io"
API_KEY = os.getenv("APIFOOTBALL_KEY", "").strip()
LOCAL_TZ = ZoneInfo("Europe/Vienna")
ACTIVE_START_HOUR = 18
ACTIVE_END_HOUR = 23
MAX_LOCAL_CALLS = 75  # reserve part of the 100/day quota for safety and match details
LIVE_CACHE_SECONDS = 600
DETAIL_CACHE_SECONDS = 600
MAX_SELECTED_MATCHES = 3

LEAGUES = {
    39: "Premier League", 140: "La Liga", 78: "Bundesliga",
    135: "Serie A", 61: "Ligue 1", 88: "Eredivisie",
    94: "Primeira Liga", 203: "Süper Lig",
    2: "UEFA Champions League", 3: "UEFA Europa League",
    848: "UEFA Europa Conference League", 5: "UEFA Nations League",
}
LEAGUE_PRIORITY = {2: 12, 3: 10, 39: 10, 140: 10, 78: 10, 135: 10, 61: 9, 848: 8, 88: 7, 94: 7, 203: 6, 5: 8}

_lock = threading.RLock()
_day = datetime.now(timezone.utc).date()
_local_calls = 0
_provider_remaining = None
_live_cache = {"at": 0, "value": None}
_today_cache = {"at": 0, "value": None}
_detail_cache = {}

def configured():
    return bool(API_KEY)

def _now():
    return datetime.now(LOCAL_TZ)

def _active_window():
    now = _now()
    return ACTIVE_START_HOUR <= now.hour < ACTIVE_END_HOUR

def _budget_status():
    with _lock:
        remaining = max(0, MAX_LOCAL_CALLS - _local_calls)
        if _provider_remaining is not None:
            remaining = min(remaining, max(0, _provider_remaining))
        return {"daily_budget": MAX_LOCAL_CALLS, "used": _local_calls,
                "remaining": remaining, "provider_remaining": _provider_remaining}

def _get(endpoint, params=None):
    global _day, _local_calls, _provider_remaining
    if not API_KEY:
        return {"errors": ["APIFOOTBALL_KEY is not configured"], "response": []}
    today_utc = datetime.now(timezone.utc).date()
    with _lock:
        if today_utc != _day:
            _day = today_utc
            _local_calls = 0
            _provider_remaining = None
        remaining = MAX_LOCAL_CALLS - _local_calls
        if _provider_remaining is not None:
            remaining = min(remaining, _provider_remaining)
        if remaining <= 0:
            return {"errors": ["Tagesbudget erreicht; API-Aufrufe sind bis zum nächsten UTC-Reset pausiert."], "response": []}
        _local_calls += 1
    q = urllib.parse.urlencode(params or {})
    url = API_BASE + endpoint + (("?" + q) if q else "")
    req = urllib.request.Request(url, headers={"x-apisports-key": API_KEY, "User-Agent": "NextStep-GoalPredictor/2.0"})
    try:
        with urllib.request.urlopen(req, timeout=12) as response:
            headers = response.headers
            left = headers.get("x-ratelimit-requests-remaining")
            if left is not None:
                try:
                    with _lock:
                        _provider_remaining = int(left)
                except ValueError:
                    pass
            return json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return {"errors": [str(exc)], "response": []}

def _team(obj):
    return {"id": obj.get("id"), "name": obj.get("name") or "Unbekannt", "logo": obj.get("logo") or ""}

def _fixture_item(f):
    league = f.get("league") or {}
    teams = f.get("teams") or {}
    goals = f.get("goals") or {}
    status = (f.get("fixture") or {}).get("status") or {}
    return {
        "fixture_id": (f.get("fixture") or {}).get("id"),
        "date": (f.get("fixture") or {}).get("date"),
        "timestamp": (f.get("fixture") or {}).get("timestamp"),
        "minute": status.get("elapsed"), "status": status.get("short"),
        "status_long": status.get("long"),
        "home": _team(teams.get("home") or {}), "away": _team(teams.get("away") or {}),
        "home_goals": goals.get("home"), "away_goals": goals.get("away"),
        "league_id": league.get("id"), "league": league.get("name") or LEAGUES.get(league.get("id"), "Wettbewerb"),
        "league_logo": league.get("logo") or "", "country": league.get("country") or "",
    }

def _parse_odds(data):
    """Return fixture_id -> compact odds metadata from /odds/live."""
    out = {}
    for item in data.get("response", []) or []:
        fixture = item.get("fixture") or {}
        fid = fixture.get("id") or item.get("fixture_id")
        if not fid:
            continue
        markets = []
        one_x_two = {}
        for bookmaker in item.get("bookmakers") or []:
            for bet in bookmaker.get("bets") or []:
                name = bet.get("name") or "Markt"
                values = []
                for value in bet.get("values") or []:
                    label = str(value.get("value") or "")
                    odd = value.get("odd")
                    if odd is not None:
                        values.append({"label": label, "odd": str(odd)})
                        if name.lower() in ("match winner", "1x2", "winner") and label.lower() in ("home", "draw", "away"):
                            one_x_two[label.lower()] = str(odd)
                if values and len(markets) < 5:
                    markets.append({"name": name, "values": values[:4]})
        out[int(fid)] = {"available": bool(markets), "market_count": len(markets),
                         "markets": markets, "one_x_two": one_x_two}
    return out

def _selection_score(match, odds):
    league_id = match.get("league_id")
    minute = match.get("minute") or 0
    score_diff = abs((match.get("home_goals") or 0) - (match.get("away_goals") or 0))
    score = LEAGUE_PRIORITY.get(league_id, 1) * 4
    if odds.get("available"):
        score += 18 + min(odds.get("market_count", 0), 5) * 2
    else:
        score -= 8
    if 15 <= minute <= 78:
        score += 8
    elif minute < 15:
        score += 2
    elif minute > 78:
        score -= 4
    if score_diff <= 1:
        score += 5
    if match.get("status") in ("HT", "BT"):
        score -= 3
    return score

def live_matches(force=False):
    now = time.time()
    with _lock:
        if _live_cache["value"] is not None and now - _live_cache["at"] < LIVE_CACHE_SECONDS and not force:
            return dict(_live_cache["value"])
    budget = _budget_status()
    if not configured():
        return {"configured": False, "matches": [], "error": "APIFOOTBALL_KEY fehlt in Render.", "budget": budget}
    if not _active_window():
        return {"configured": True, "active": False, "matches": [],
                "message": "GoalPredictor ist nur zwischen 18:00 und 23:00 Uhr (Wien) aktiv.",
                "budget": budget, "active_window": "18:00–23:00 Europe/Vienna"}
    ids = "-".join(str(x) for x in LEAGUES)
    fixtures = _get("/fixtures", {"live": ids, "timezone": "Europe/Vienna"})
    raw_matches = [_fixture_item(x) for x in fixtures.get("response", []) or []]
    if not raw_matches:
        result = {"configured": True, "active": True, "matches": [], "selected_count": 0,
                  "selection_note": "Keine passenden Live-Spiele gefunden.",
                  "errors": fixtures.get("errors", []), "budget": _budget_status(),
                  "active_window": "18:00–23:00 Europe/Vienna"}
    else:
        odds_data = _get("/odds/live")
        odds_map = _parse_odds(odds_data)
        for m in raw_matches:
            od = odds_map.get(int(m["fixture_id"]), {"available": False, "market_count": 0, "markets": [], "one_x_two": {}})
            m["odds"] = od
            m["selection_score"] = _selection_score(m, od)
            if od.get("available"):
                m["selection_reason"] = "Live-Quoten verfügbar · Top-Wettbewerb"
            else:
                m["selection_reason"] = "Top-Wettbewerb · Quoten derzeit nicht geliefert"
        # Prioritize matches with odds, major competitions, useful match minute and close score.
        raw_matches.sort(key=lambda m: (m["selection_score"], m.get("timestamp") or 0), reverse=True)
        selected = raw_matches[:MAX_SELECTED_MATCHES]
        for rank, m in enumerate(selected, 1):
            m["selection_rank"] = rank
        result = {"configured": True, "active": True, "matches": selected,
                  "selected_count": len(selected), "available_live_count": len(raw_matches),
                  "selection_note": "Automatische Vorauswahl nach Quotenverfügbarkeit, Wettbewerb, Spielminute und Spielstand. Das ist kein garantierter Value-Bet-Nachweis.",
                  "errors": (fixtures.get("errors", []) or []) + (odds_data.get("errors", []) or []),
                  "budget": _budget_status(), "active_window": "18:00–23:00 Europe/Vienna"}
    with _lock:
        _live_cache["at"] = now
        _live_cache["value"] = result
    return dict(result)

def today_matches():
    if not configured():
        return {"configured": False, "matches": [], "error": "APIFOOTBALL_KEY fehlt in Render.", "budget": _budget_status()}
    if not _active_window():
        return {"configured": True, "active": False, "matches": [],
                "message": "GoalPredictor ist nur zwischen 18:00 und 23:00 Uhr (Wien) aktiv.",
                "budget": _budget_status(), "active_window": "18:00–23:00 Europe/Vienna"}
    now = time.time()
    with _lock:
        if _today_cache["value"] is not None and now - _today_cache["at"] < LIVE_CACHE_SECONDS:
            return dict(_today_cache["value"])
    today = _now().date().isoformat()
    data = _get("/fixtures", {"date": today, "timezone": "Europe/Vienna"})
    matches = [_fixture_item(x) for x in (data.get("response", []) or []) if (x.get("league") or {}).get("id") in LEAGUES]
    matches.sort(key=lambda x: (-(LEAGUE_PRIORITY.get(x.get("league_id"), 1)), x.get("date") or ""))
    result = {"configured": True, "active": True, "matches": matches[:MAX_SELECTED_MATCHES],
              "date": today, "selected_count": min(len(matches), MAX_SELECTED_MATCHES),
              "errors": data.get("errors", []), "budget": _budget_status(),
              "active_window": "18:00–23:00 Europe/Vienna"}
    with _lock:
        _today_cache["at"] = now
        _today_cache["value"] = result
    return dict(result)

def _num(value):
    if value is None: return 0.0
    if isinstance(value, (int, float)): return float(value)
    try: return float(str(value).replace("%", "").replace(",", "").strip())
    except Exception: return 0.0

def _stats_to_map(stats_response):
    out = {}
    for team_block in stats_response.get("response", []) or []:
        tid = str((team_block.get("team") or {}).get("id"))
        out[tid] = {item.get("type"): item.get("value") for item in (team_block.get("statistics") or [])}
    return out

def match_detail(fixture_id):
    if not configured():
        return {"configured": False, "error": "APIFOOTBALL_KEY fehlt in Render.", "budget": _budget_status()}
    try:
        fid = int(fixture_id)
    except (TypeError, ValueError):
        return {"configured": True, "error": "Ungültige Spiel-ID.", "budget": _budget_status()}
    now = time.time()
    with _lock:
        cached = _detail_cache.get(fid)
        if cached and now - cached["at"] < DETAIL_CACHE_SECONDS:
            return dict(cached["value"])
    if not _active_window():
        return {"configured": True, "active": False,
                "error": "Live-Details sind nur zwischen 18:00 und 23:00 Uhr (Wien) verfügbar.",
                "budget": _budget_status()}
    # One batched fixtures call; avoid separate statistics/events calls that would burn the free quota.
    data = _get("/fixtures", {"ids": str(fid), "timezone": "Europe/Vienna"})
    response = data.get("response", []) or []
    if not response:
        return {"configured": True, "error": "Spiel nicht gefunden oder Tagesbudget erreicht.",
                "errors": data.get("errors", []), "budget": _budget_status()}
    f = response[0]
    base = _fixture_item(f)
    home_id = str(base["home"]["id"]); away_id = str(base["away"]["id"])
    stat_map = _stats_to_map({"response": f.get("statistics") or []})
    home_stats, away_stats = stat_map.get(home_id, {}), stat_map.get(away_id, {})
    metrics = {
        "shots": {"home": _num(home_stats.get("Total Shots")), "away": _num(away_stats.get("Total Shots"))},
        "shots_on_target": {"home": _num(home_stats.get("Shots on Goal")), "away": _num(away_stats.get("Shots on Goal"))},
        "corners": {"home": _num(home_stats.get("Corner Kicks")), "away": _num(away_stats.get("Corner Kicks"))},
        "possession": {"home": _num(home_stats.get("Ball Possession")), "away": _num(away_stats.get("Ball Possession"))},
        "dangerous_attacks": {"home": _num(home_stats.get("Dangerous Attacks")), "away": _num(away_stats.get("Dangerous Attacks"))},
        "xg": {"home": _num(home_stats.get("expected_goals")), "away": _num(away_stats.get("expected_goals"))},
    }
    events = f.get("events") or []
    hg, ag, minute = _num(base["home_goals"]), _num(base["away_goals"]), _num(base["minute"])
    ha = .45 + .020*metrics["shots"]["home"] + .055*metrics["shots_on_target"]["home"] + .070*metrics["xg"]["home"] + .002*metrics["dangerous_attacks"]["home"] + .010*metrics["corners"]["home"]
    aa = .40 + .020*metrics["shots"]["away"] + .055*metrics["shots_on_target"]["away"] + .070*metrics["xg"]["away"] + .002*metrics["dangerous_attacks"]["away"] + .010*metrics["corners"]["away"]
    total = max(.001, ha + aa)
    intensity = min(.96, max(.03, .16*max(.25, (90-minute)/90) + .045*total))
    p_next_home, p_next_away = intensity*ha/total, intensity*aa/total
    p_next_none = max(0.0, 1-p_next_home-p_next_away)
    diff = hg-ag
    import math
    def softmax(vals):
        m=max(vals); e=[math.exp(v-m) for v in vals]; s=sum(e); return [v/s for v in e]
    p_home,p_draw,p_away = softmax([.25+.70*diff+.18*(ha-aa), .20-.25*abs(diff)+.03*max(0,(90-minute))/90, -.05-.70*diff+.18*(aa-ha)])
    rate=max(.002,intensity/max(1,90-minute))
    p5, p10 = 1-math.exp(-rate*5), 1-math.exp(-rate*10)
    result = {"configured": True, "match": base, "metrics": metrics, "events": events[-8:],
              "prediction": {"p_home": round(p_home,4), "p_draw": round(p_draw,4), "p_away": round(p_away,4),
                "p_next_home": round(p_next_home,4), "p_next_away": round(p_next_away,4), "p_next_none": round(p_next_none,4),
                "p_goal_5m": round(p5,4), "p_goal_10m": round(p10,4),
                "model_version": "GoalPredictor V2 live-baseline"},
              "note": "Baseline-Schätzung, keine validierte Wettprognose. Statistikfelder können im Free-Tarif fehlen.",
              "budget": _budget_status(), "errors": data.get("errors", [])}
    with _lock:
        _detail_cache[fid] = {"at": now, "value": result}
    return result
