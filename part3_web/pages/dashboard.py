from flask import Blueprint, render_template

from db import DatabaseError, query

bp = Blueprint("dashboard", __name__)
NAV_TITLE = "Dashboard"

MIN_INJURIES = 10
TABS = [
    ("body_part", "Body Part"),
    ("severity", "Severity"),
    ("roster_position", "Position"),
    ("weather", "Weather"),
    ("temperature_band", "Temperature"),
    ("stadium", "Stadium Type"),
]
SKILL = {"Wide Receiver", "Running Back", "Tight End", "Cornerback", "Safety"}
LINEMEN = {"Offensive Lineman", "Defensive Lineman"}
ANKLE_FOOT = {"Ankle", "Foot", "Toes", "Heel"}
SIDES = {"Synthetic": "syn", "Natural": "nat"}


def rate(injuries, plays):
    return injuries / plays * 1000 if plays else None


def ratio(a, b):
    return a / b if a is not None and b else None


def load_overall():
    rows = query(
        "SELECT field_type, plays, injuries FROM injury_rates WHERE group_type = %s",
        ("overall",),
    )
    sides = {SIDES[r["field_type"]]: r for r in rows if r["field_type"] in SIDES}
    if len(sides) < 2:
        return None
    for r in sides.values():
        r["rate"] = rate(r["injuries"], r["plays"])
    sides["ratio"] = ratio(sides["syn"]["rate"], sides["nat"]["rate"])
    sides["plays"] = sides["syn"]["plays"] + sides["nat"]["plays"]
    return sides


def load_tabs():
    keys = [key for key, _ in TABS]
    placeholders = ", ".join(["%s"] * len(keys))
    rows = query(
        "SELECT group_type, group_value, field_type, plays, injuries"
        f" FROM injury_rates WHERE group_type IN ({placeholders})",
        keys,
    )
    groups = {key: {} for key in keys}
    for r in rows:
        if r["field_type"] in SIDES:
            g = groups[r["group_type"]].setdefault(r["group_value"], {"value": r["group_value"]})
            g[SIDES[r["field_type"]]] = {
                "injuries": r["injuries"],
                "plays": r["plays"],
                "rate": rate(r["injuries"], r["plays"]),
            }
    tabs = []
    for key, label in TABS:
        # A group needs both surfaces to be compared at all
        items = [g for g in groups[key].values() if "syn" in g and "nat" in g]
        for g in items:
            g["ratio"] = ratio(g["syn"]["rate"], g["nat"]["rate"])
            g["few"] = min(g["syn"]["injuries"], g["nat"]["injuries"]) < MIN_INJURIES
        items.sort(key=lambda g: (g["value"] == "unknown", g["value"]))
        tabs.append({"key": key, "label": label, "groups": items})
    return tabs


def tab_groups(tabs, key):
    return next((t["groups"] for t in tabs if t["key"] == key), [])


def position_gap(tabs):
    # H3: pool skill positions and linemen so each side has more injuries
    out = {}
    for name, members in (("skill", SKILL), ("linemen", LINEMEN)):
        chosen = [g for g in tab_groups(tabs, "roster_position") if g["value"] in members]
        if not chosen:
            continue
        side = {}
        for s in ("syn", "nat"):
            injuries = sum(g[s]["injuries"] for g in chosen)
            side[s] = {"injuries": injuries, "rate": rate(injuries, sum(g[s]["plays"] for g in chosen))}
        side["ratio"] = ratio(side["syn"]["rate"], side["nat"]["rate"])
        side["few"] = min(side["syn"]["injuries"], side["nat"]["injuries"]) < MIN_INJURIES
        out[name] = side
    return out


def ankle_foot_share(tabs):
    # H4: how much of the turf-minus-grass gap comes from ankle/foot injuries
    extra = {
        g["value"]: g["syn"]["rate"] - g["nat"]["rate"]
        for g in tab_groups(tabs, "body_part")
        if g["syn"]["rate"] is not None and g["nat"]["rate"] is not None
    }
    total = sum(extra.values())
    if total <= 0:
        return None
    part = sum(v for k, v in extra.items() if k in ANKLE_FOOT)
    return {"total": total, "ankle_foot": part, "share": part / total}


def load_movement():
    # Averages weighted by plays so big position groups count more
    rows = query(
        "SELECT field_type, SUM(plays) AS plays,"
        " SUM(avg_max_speed * plays) / SUM(plays) AS max_speed,"
        " SUM(avg_sharp_cuts * plays) / SUM(plays) AS sharp_cuts"
        " FROM movement_summary WHERE field_type IN (%s, %s) GROUP BY field_type",
        tuple(SIDES),
    )
    sides = {SIDES[r["field_type"]]: r for r in rows}
    return sides if len(sides) == 2 else None


def load_league():
    rows = query(
        "SELECT season, surface, SUM(injuries) AS injuries, SUM(games) AS games"
        " FROM league_injury_rates WHERE surface IN (%s, %s)"
        " GROUP BY season, surface ORDER BY season",
        tuple(SIDES),
    )
    if not rows:
        return None
    seasons = sorted({r["season"] for r in rows})
    out = {"seasons": seasons}
    for surface, s in SIDES.items():
        mine = {r["season"]: r for r in rows if r["surface"] == surface}
        out[s] = [ratio(mine[y]["injuries"], mine[y]["games"]) if y in mine else None for y in seasons]
        out[s + "_avg"] = ratio(sum(r["injuries"] for r in mine.values()),
                                sum(r["games"] for r in mine.values()))
    return out


@bp.route("/dashboard")
def index():
    try:
        overall = load_overall()
        tabs = load_tabs()
        movement = load_movement()
        league = load_league()
    except DatabaseError as e:
        return render_template(
            "dashboard.html", error=f"Couldn't load the dashboard data. {e}",
            overall=None, tabs=[], movement=None, league=None,
            gap={}, ankle=None, min_injuries=MIN_INJURIES,
        ), 503
    return render_template(
        "dashboard.html", overall=overall, tabs=tabs, movement=movement,
        league=league, gap=position_gap(tabs), ankle=ankle_foot_share(tabs),
        min_injuries=MIN_INJURIES,
    )
