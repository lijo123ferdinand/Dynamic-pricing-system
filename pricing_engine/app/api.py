from flask import Flask, jsonify, request

from app.config import Config
from app.utils.logging_utils import get_logger
from app.optimizer.price_optimizer import (
    optimize_price_for_sku,
    persist_optimization_result,
    log_prediction,
)
from app.feedback.feedback_handler import save_feedback
from app.models.demand_model import load_demand_model
from app.models.elasticity import get_elasticity_for_sku

logger = get_logger(__name__)

def create_app() -> Flask:
    app = Flask(__name__)

    # Load models at startup
    with app.app_context():
        try:
            load_demand_model()
            logger.info("Demand model loaded at startup.")
        except Exception as e:
            logger.error(f"Failed to load demand model: {e}")

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok"}), 200

    @app.route("/models/status", methods=["GET"])
    def models_status():
        # Simplified: check if demand model file exists
        import os

        demand_exists = os.path.exists(Config.DEMAND_MODEL_PATH)
        return jsonify(
            {
                "demand_model": {
                    "loaded": demand_exists,
                    "path": Config.DEMAND_MODEL_PATH,
                }
            }
        )

    @app.route("/price-suggestions", methods=["GET"])
    def price_suggestions():
        sku = request.args.get("sku")
        vendor_id = request.args.get("vendor_id", "default_vendor")

        if not sku:
            return jsonify({"error": "sku is required"}), 400

        result = optimize_price_for_sku(sku, vendor_id)
        if not result:
            return jsonify({"error": "No suggestion available"}), 404

        suggestion_id = persist_optimization_result(result)

        # For logging, we log only key features
        input_features = {
            "sku": result.sku,
            "vendor_id": result.vendor_id,
            "current_price": result.current_price,
        }
        output = {
            "optimal_price": result.optimal_price,
            "expected_revenue": result.expected_revenue,
            "expected_profit": result.expected_profit,
        }
        log_prediction(
            sku=result.sku,
            vendor_id=result.vendor_id,
            suggestion_id=suggestion_id,
            model_type="optimizer",
            input_features=input_features,
            output=output,
        )

        response = {
            "sku": result.sku,
            "current_price": result.current_price,
            "suggested_price": result.optimal_price,
            "expected_revenue": result.expected_revenue,
            "expected_profit": result.expected_profit,
            "elasticity": result.elasticity,
            "confidence": result.confidence,
            "reason": result.reason,
            "actions": ["accept", "reject", "custom_price"],
        }
        return jsonify(response), 200

    @app.route("/price-feedback", methods=["POST"])
    def price_feedback():
        data = request.get_json(force=True)
        required = ["vendor_id", "sku", "suggested_price", "action", "timestamp"]
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({"error": f"Missing fields: {','.join(missing)}"}), 400

        if data["action"] not in ("accept", "reject", "custom_price"):
            return jsonify({"error": "Invalid action"}), 400

        if data["action"] == "custom_price" and not data.get("custom_price"):
            return jsonify({"error": "custom_price required for action=custom_price"}), 400

        save_feedback(data)
        return jsonify({"status": "ok"}), 200

    return app
