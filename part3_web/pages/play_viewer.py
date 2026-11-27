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
