"""Pivot web app. Each module in pages/ defines a Blueprint named bp; set
NAV_TITLE to put it in the nav, which links to the blueprint's "index" view."""
import importlib
import os
import pkgutil

from flask import Flask, redirect, render_template

from db import DatabaseError

NAV_ORDER = ["Dashboard", "Search", "Stadium Map", "Play Viewer"]
PAGES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pages")

app = Flask(__name__)
nav_links = []

for info in pkgutil.iter_modules([PAGES_DIR]):
    module = importlib.import_module(f"pages.{info.name}")
    if hasattr(module, "bp"):
        app.register_blueprint(module.bp)
        if hasattr(module, "NAV_TITLE"):
            nav_links.append((module.NAV_TITLE, f"{module.bp.name}.index"))

order = {title: i for i, title in enumerate(NAV_ORDER)}
nav_links.sort(key=lambda link: (order.get(link[0], len(order)), link[0]))
app.jinja_env.globals["nav_links"] = nav_links


@app.route("/")
def home():
    return redirect("/dashboard")


@app.errorhandler(DatabaseError)
def database_error(e):
    return render_template("base.html", error=f"Database problem: {e}"), 503


@app.errorhandler(404)
def not_found(e):
    return render_template("base.html", error="Page not found."), 404


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "5000")))
