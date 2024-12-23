from flask import Blueprint, jsonify, request, render_template, send_from_directory
from flask_api.analysis_services import *
import os

# analysis_blueprint = Blueprint("analysis", __name__, url_prefix="/api/analysis")
analysis_blueprint = Blueprint("analysis", __name__, static_folder="static")


@analysis_blueprint.route("/", methods=["GET"])
def run_selected_query():
    query = request.args.get("query")
    top = request.args.get("top", type=int)

    # מיפוי בין השמות בפורם לפונקציות
    query_map = {
        "deadliest": get_deadliest_attack_types,
        "casualties_by_country": get_top_countries_by_casualties,
        "top_groups": get_top_5_group_by_casualties,
        "diff_percentage": get_diff_percentage_by_year_and_country,
        "active_groups": get_most_active_groups_by_country,
        "common_target": get_max_groups_with_common_target_by_country,
    }

    if query in query_map:
        # קריאה לפונקציה המתאימה
        return query_map[query]()

    # במקרה שהשאילתה לא נמצאה
    return jsonify({"error": "Invalid query selected"}), 400


@analysis_blueprint.route("/flask_api/static/maps/map.html")
def render_map():
    static_folder = os.path.join("flask_api", "static", "maps")
    return send_from_directory(static_folder, "map.html")

@analysis_blueprint.route("/deadliest", methods=["GET"])
def get_deadliest_attack_types():
    top = request.args.get("top", default=None)
    attack_types = order_by_attack_types_deadliest(top)
    return jsonify({"attack types": attack_types}), 200


@analysis_blueprint.route("/casualties_by_country", methods=["GET"])
def get_top_countries_by_casualties():
    top = request.args.get("top", default=None)
    calculate_top_countries_by_casualties(top)
    return render_template("index.html")
    # return jsonify({"average countries": avg_countries}), 200


@analysis_blueprint.route("/top_groups", methods=["GET"])
def get_top_5_group_by_casualties():
    groups = find_top_5_group_by_casualties()
    return jsonify({"Groups": groups}), 200



@analysis_blueprint.route("/diff_percentage", methods=["GET"])
def get_diff_percentage_by_year_and_country():
    years = calc_diff_percentage_by_year_and_country()
    return render_template("index.html")
    # return jsonify({"Years": years}), 200


@analysis_blueprint.route("/active_groups", methods=["GET"])
def get_most_active_groups_by_country():
    countries = find_most_active_groups_by_country()
    return render_template("index.html")
    # return jsonify({"Countries": countries}), 200


@analysis_blueprint.route("/common_target", methods=["GET"])
def get_max_groups_with_common_target_by_country():
    country = find_max_groups_with_common_target_by_country()
    return jsonify({"Country": country}), 200

