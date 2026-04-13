import os
from flask import Flask, render_template, redirect, url_for, request, abort
from flask_httpauth import HTTPBasicAuth
from dotenv import load_dotenv
from models import init_db, get_session, Listing
from sqlalchemy import desc, text

load_dotenv()

app = Flask(__name__)
auth = HTTPBasicAuth()

APP_USER = os.environ.get("APP_USER", "asma")
APP_PASS = os.environ.get("APP_PASS", "changeme")


@auth.verify_password
def verify(username, password):
    return username == APP_USER and password == APP_PASS


# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────

@app.route("/")
@auth.login_required
def index():
    session = get_session()
    track        = request.args.get("track", "")
    min_fit      = int(request.args.get("min_fit", 5))
    show_applied = request.args.get("show_applied") == "1"

    q = (
        session.query(Listing)
        .filter(Listing.dismissed == False)
        .filter(Listing.scored == True)
        .filter(Listing.fit_score >= min_fit)
    )
    if track:
        q = q.filter(Listing.best_track == track)
    if not show_applied:
        q = q.filter(Listing.applied == False)

    listings = q.order_by(desc(Listing.fit_score), desc(Listing.date_posted)).all()
    session.close()

    tracks = ["Threat Intel", "Fraud/T&S", "Detection Engineering", "Incident Ops"]
    return render_template(
        "index.html",
        listings=listings,
        tracks=tracks,
        selected_track=track,
        min_fit=min_fit,
        show_applied=show_applied,
    )


@app.route("/listing/<int:listing_id>")
@auth.login_required
def listing_detail(listing_id):
    session = get_session()
    listing = session.get(Listing, listing_id)
    if not listing:
        abort(404)
    session.close()
    return render_template("listing.html", listing=listing)


@app.route("/action/<int:listing_id>/<action>", methods=["POST"])
@auth.login_required
def action(listing_id, action):
    session = get_session()
    listing = session.get(Listing, listing_id)
    if not listing:
        abort(404)

    if action == "dismiss":
        listing.dismissed = True
    elif action == "applied":
        listing.applied = True
    elif action == "unapply":
        listing.applied = False

    session.commit()
    session.close()

    next_url = request.form.get("next", url_for("index"))
    return redirect(next_url)


@app.route("/reset-db-asma-only")
@auth.login_required
def reset_db():
    session = get_session()
    session.execute(text("DELETE FROM listings"))
    session.commit()
    session.close()
    return "cleared"


# ─────────────────────────────────────────────
# Init and run
# ─────────────────────────────────────────────

init_db()

if __name__ == "__main__":
    app.run(debug=True, port=5000)
