"""GoalPredictor live data adapter for NextStep.
Uses API-Football through a server-side API key.
No secret belongs in source control.
"""
import os, json, urllib.request, urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo

API_BASE = "https://v3.football.api-sports.io"
API_KEY = os.getenv("APIFOOTBALL_KEY", "").strip()

LEAGUES = {
    39: "Premier League",
    140: "La Liga",
    78: "Bundesliga",
    135: "Serie A",
    61: "Ligue 1",
    88: "Eredivisie",
    94: "Primeira Liga",
    203: "Süper Lig",
    2: "UEFA Champions League",
    3: "UEFA Europa League",
    848: "UEFA Europa Conference League",
    5: "UEFA Nations League",
}

def configured():
    return bool(API_KEY)

def _get(endpoint, params=None):
    if not API_KEY:
        return {"errors": ["APIFOOTBALL_KEY is not configured"], "response": []}
    q = urllib.parse.urlencode(params or {})
    url = API_BASE + endpoint + (("?" + q) if q else "")
    req = urllib.request.Request(url, headers={"x-apisports-key": API_KEY, "User-Agent": "NextStep-GoalPredictor/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=12) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return {"errors": [str(exc)], "response": []}

def _team(obj):
    return {
        "id": obj.get("id"),
        "name": obj.get("name") or "Unbekannt",
        "logo": obj.get("logo") or "",
    }

def _fixture_item(f):
    league = f.get("league") or {}
    teams = f.get("teams") or {}
    goals = f.get("goals") or {}
    status = f.get("fixture", {}).get("status") or {}
    return {
        "fixture_id": f.get("fixture", {}).get("id"),
        "date": f.get("fixture", {}).get("date"),
        "timestamp": f.get("fixture", {}).get("timestamp"),
        "minute": status.get("elapsed"),
        "status": status.get("short"),
        "status_long": status.get("long"),
        "home": _team(teams.get("home") or {}),
        "away": _team(teams.get("away") or {}),
        "home_goals": goals.get("home"),
        "away_goals": goals.get("away"),
        "league_id": league.get("id"),
        "league": league.get("name") or LEAGUES.get(league.get("id"), "Wettbewerb"),
        "league_logo": league.get("logo") or "",
        "country": league.get("country") or "",
    }

def live_matches():
    if not configured():
        return {"configured": False, "matches": [], "error": "APIFOOTBALL_KEY fehlt in Render."}
    ids = "-".join(str(x) for x in LEAGUES)
    data = _get("/fixtures", {"live": ids, "timezone": "Europe/Vienna"})
    matches = [_fixture_item(x) for x in data.get("response", [])]
    return {"configured": True, "matches": matches, "leagues": LEAGUES, "errors": data.get("errors", [])}

def today_matches():
    if not configured():
        return {"configured": False, "matches": [], "error": "APIFOOTBALL_KEY fehlt in Render."}
    today = datetime.now(ZoneInfo("Europe/Vienna")).date().isoformat()
    ids = "-".join(str(x) for x in LEAGUES)
    data = _get("/fixtures", {"date": today, "timezone": "Europe/Vienna"})
    matches = [
        _fixture_item(x) for x in data.get("response", [])
        if (x.get("league") or {}).get("id") in LEAGUES
    ]
    matches.sort(key=lambda x: (x.get("date") or "", x.get("league") or ""))
    return {"configured": True, "matches": matches, "date": today, "leagues": LEAGUES, "errors": data.get("errors", [])}

def _stats_to_map(stats_response):
    out = {"home": {}, "away": {}}
    for team_block in stats_response.get("response", []):
        side = "home" if (team_block.get("team") or {}).get("id") == team_block.get("_home_id") else None
        # API does not include an explicit home/away flag, so caller maps by team id.
        out.setdefault(str((team_block.get("team") or {}).get("id")), {})
        for item in team_block.get("statistics") or []:
            name = item.get("type")
            val = item.get("value")
            out[str((team_block.get("team") or {}).get("id"))][name] = val
    return out

def _num(value):
    if value is None:
        return 0.0
    if isinstance(value, (int,float)):
        return float(value)
    text = str(value).replace("%","").replace(",","").strip()
    try: return float(text)
    except Exception: return 0.0

def match_detail(fixture_id):
    if not configured():
        return {"configured": False, "error": "APIFOOTBALL_KEY fehlt in Render."}
    fixture = _get("/fixtures", {"id": fixture_id, "timezone": "Europe/Vienna"})
    if not fixture.get("response"):
        return {"configured": True, "error": "Spiel nicht gefunden.", "errors": fixture.get("errors", [])}
    f = fixture["response"][0]
    base = _fixture_item(f)
    home_id = base["home"]["id"]; away_id = base["away"]["id"]

    stats = _get("/fixtures/statistics", {"fixture": fixture_id})
    stat_map = _stats_to_map(stats)
    home_stats = stat_map.get(str(home_id), {})
    away_stats = stat_map.get(str(away_id), {})

    events = _get("/fixtures/events", {"fixture": fixture_id})
    ev = events.get("response", [])

    def stat(d, name):
        return d.get(name)

    # Extract common live metrics. xG is present when the competition supplies it.
    metrics = {
        "shots": {"home": _num(stat(home_stats,"Total Shots")), "away": _num(stat(away_stats,"Total Shots"))},
        "shots_on_target": {"home": _num(stat(home_stats,"Shots on Goal")), "away": _num(stat(away_stats,"Shots on Goal"))},
        "corners": {"home": _num(stat(home_stats,"Corner Kicks")), "away": _num(stat(away_stats,"Corner Kicks"))},
        "possession": {"home": _num(stat(home_stats,"Ball Possession")), "away": _num(stat(away_stats,"Ball Possession"))},
        "dangerous_attacks": {"home": _num(stat(home_stats,"Dangerous Attacks")), "away": _num(stat(away_stats,"Dangerous Attacks"))},
        "xg": {"home": _num(stat(home_stats,"expected_goals")), "away": _num(stat(away_stats,"expected_goals"))},
    }

    hg = _num(base["home_goals"]); ag = _num(base["away_goals"])
    minute = _num(base["minute"])
    ha = .45 + .020*metrics["shots"]["home"] + .055*metrics["shots_on_target"]["home"] + .070*metrics["xg"]["home"] + .002*metrics["dangerous_attacks"]["home"] + .010*metrics["corners"]["home"]
    aa = .40 + .020*metrics["shots"]["away"] + .055*metrics["shots_on_target"]["away"] + .070*metrics["xg"]["away"] + .002*metrics["dangerous_attacks"]["away"] + .010*metrics["corners"]["away"]
    total = max(0.001, ha+aa)
    intensity = min(.96, max(.03, .16*max(.25,(90-minute)/90) + .045*total))
    p_next_home = intensity*ha/total
    p_next_away = intensity*aa/total
    p_next_none = max(0.0, 1-p_next_home-p_next_away)

    diff = hg-ag
    def softmax(vals):
        import math
        m=max(vals); e=[math.exp(v-m) for v in vals]; s=sum(e); return [v/s for v in e]
    p_home,p_draw,p_away = softmax([
        .25+.70*diff+.18*(ha-aa),
        .20-.25*abs(diff)+.03*max(0,(90-minute))/90,
        -.05-.70*diff+.18*(aa-ha)
    ])
    rate=max(.002,intensity/max(1,90-minute))
    import math
    p5=1-math.exp(-rate*5)
    p10=1-math.exp(-rate*10)

    return {
        "configured": True,
        "match": base,
        "metrics": metrics,
        "events": ev,
        "prediction": {
            "p_home": round(p_home,4), "p_draw": round(p_draw,4), "p_away": round(p_away,4),
            "p_next_home": round(p_next_home,4), "p_next_away": round(p_next_away,4), "p_next_none": round(p_next_none,4),
            "p_goal_5m": round(p5,4), "p_goal_10m": round(p10,4),
            "model_version": "GoalPredictor V2 live-baseline"
        },
        "errors": stats.get("errors", []) + events.get("errors", [])
    }
}
