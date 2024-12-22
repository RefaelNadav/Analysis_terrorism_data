from flask import Blueprint, jsonify
from flask_api.analysis_services import *

analysis_blueprint = Blueprint("analysis", __name__, url_prefix="/api/analysis")


@analysis_blueprint.route("/deadliest", methods=["GET"])
def get_deadliest_attack_types():
    # limit = request.args.get("limit")
    attack_types = order_by_attack_types_deadliest()
    return jsonify({"attack types": attack_types}), 200


@analysis_blueprint.route("/casualties_by_country", methods=["GET"])
def calculate_top_countries_by_casualties():
    # limit = request.args.get("limit")
    avg_countries = fcalculate_top_countries_by_casualties()
    return jsonify({"average countries": avg_countries}), 200


@analysis_blueprint.route("/top_groups", methods=["GET"])
def get_top_5_group_by_casualties():
    groups = find_top_5_group_by_casualties()
    return jsonify({"Groups": groups}), 200



@analysis_blueprint.route("/diff_percentage", methods=["GET"])
def get_diff_percentage_by_year_and_country():
    years = calc_diff_percentage_by_year_and_country()
    return jsonify({"Years": years}), 200


@analysis_blueprint.route("/active_groups", methods=["GET"])
def get_most_active_groups_by_country():
    countries = find_most_active_groups_by_country()
    return jsonify({"Countries": countries}), 200
