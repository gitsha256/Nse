from flask import Flask, request, jsonify, send_file
from processing import process_trade_date, process_date_range

app = Flask(__name__)

@app.route('/process-date', methods=['POST'])
def process_single_date():
    """Process data for a specific date."""
    data = request.json
    trade_date = data.get("trade_date")
    if not trade_date:
        return jsonify({"error": "trade_date is required"}), 400

    try:
        output_file = process_trade_date(trade_date)
        return jsonify({"message": "Data processed successfully", "file": output_file}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/process-range', methods=['POST'])
def process_range():
    """Process data for a range of dates."""
    data = request.json
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    if not start_date or not end_date:
        return jsonify({"error": "start_date and end_date are required"}), 400

    try:
        process_date_range(start_date, end_date)
        return jsonify({"message": "Date range processed successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    """Download processed data file."""
    try:
        return send_file(f"data/{filename}", as_attachment=True)
    except FileNotFoundError:
        return jsonify({"error": "File not found"}), 404


if __name__ == '__main__':
    app.run(debug=True)
