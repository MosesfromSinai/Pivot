from flask import Blueprint, render_template, request
from db import query



def find_sharp_cuts(tracks):
    cuts = []
    previous_candidate = False

    for i in range(5, len(tracks)):
        current = tracks[i]
        previous = tracks[i - 5]

        candidate = False

        if all(value is not None for value in [
            current["dir"], previous["dir"],
            current["speed"], previous["speed"]
        ]):
            elapsed = current["time"] - previous["time"]

            angle = abs(current["dir"] - previous["dir"]) % 360
            angle = min(angle, 360 - angle)

            candidate = (
                abs(elapsed - 0.5) < 0.05
                and angle >= 45
                and current["speed"] >= 1.0
                and previous["speed"] >= 1.0
            )

        if candidate and not previous_candidate:
            cuts.append(current)

        previous_candidate = candidate

    return cuts


def index():
    key = request.args.get("key", "")
    mode = request.args.get("type", "injury")

    if mode not in ("injury", "normal"):
        mode = "injury"

    # Find plays with available tracking data.
    available = query("""
        SELECT p.play_key, p.player_key,
               p.roster_position, p.field_type,
               p.is_injury
        FROM play_features p
        WHERE EXISTS (
            SELECT 1 FROM play_tracks t
            WHERE t.play_key = p.play_key
        )
        ORDER BY p.play_key
    """)

    selected = next(
        (p for p in available if p["play_key"] == key),
        None
    )

    if selected:
        mode = "injury" if selected["is_injury"] else "normal"

    injury_mode = mode == "injury"

    plays = [
        p for p in available
        if bool(p["is_injury"]) == injury_mode
    ]

    # Default to the first available play.
    if not key and plays:
        key = plays[0]["play_key"]

    return plays, key, mode
