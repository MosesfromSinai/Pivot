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
