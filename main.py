from flask import Flask, request, jsonify
from processing import process_trade_date, process_date_range

app = Flask(__name__)

@app.route('/process', methods=['GET'])
def process_data():
    start_date_input = request.args.get('start_date')
    end_date_input = request.args.get('end_date')

    if start_date_input and end_date_input:
        try:
            start_date = datetime.strptime(start_date_input, '%d-%m-%Y')
            end_date = datetime.strptime(end_date_input, '%d-%m-%Y')

            if start_date > end_date:
                return jsonify({"error": "Start date cannot be after the end date."}), 400

            process_date_range(start_date, end_date)
            return jsonify({"message": "Data processed successfully."})

        except ValueError:
            return jsonify({"error": "Invalid date format. Please use DD-MM-YYYY."}), 400
    else:
        return jsonify({"error": "Start and End date are required."}), 400

if __name__ == '__main__':
    app.run(debug=True)
