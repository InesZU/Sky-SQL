from flask import Flask, jsonify, request
from data import FlightData
import json

app = Flask(__name__)
SQLITE_URI = 'sqlite:///flights.sqlite3'
data_manager = FlightData(SQLITE_URI)


def save_to_json_file(data, filename='flights.json'):
    """Utility function to save data to a JSON file."""
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)


@app.route('/flight/<int:flight_id>', methods=['GET'])
def get_flight_by_id(flight_id):
    """
    Get flight details by flight ID.
    Returns a JSON response.
    """
    try:
        result = data_manager.get_flight_by_id(flight_id)
        if not result:
            return jsonify({'error': 'Flight not found'}), 404
        return jsonify([dict(row._mapping) for row in result])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/flights/date', methods=['GET'])
def get_flights_by_date():
    """
    Get flights by date.
    Returns a JSON response and saves the result to flights.json.
    """
    day = request.args.get('day', type=int)
    month = request.args.get('month', type=int)
    year = request.args.get('year', type=int)

    if not all([day, month, year]):
        return jsonify({"error": "Invalid date parameters"}), 400

    try:
        results = data_manager.get_flights_by_date(day, month, year)
        flights_data = [dict(row._mapping) for row in results]
        save_to_json_file(flights_data)  # Save to JSON file
        return jsonify(flights_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/flights/delayed/airline', methods=['GET'])
def get_delayed_flights_by_airline():
    """
    Get delayed flights by airline.
    Returns a JSON response and saves the result to flights.json.
    """
    airline = request.args.get('airline')
    if not airline:
        return jsonify({"error": "Airline parameter is required"}), 400

    try:
        results = data_manager.get_delayed_flights_by_airline(airline)
        delayed_flights_data = [dict(row._mapping) for row in results]
        save_to_json_file(delayed_flights_data)  # Save to JSON file
        return jsonify(delayed_flights_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/flights/delayed/airport', methods=['GET'])
def get_delayed_flights_by_airport():
    """
    Get delayed flights by airport.
    Returns a JSON response and saves the result to flights.json.
    """
    airport = request.args.get('airport')
    if not airport or len(airport) != 3:
        return jsonify({"error": "Valid 3-letter IATA airport code is required"}), 400

    try:
        results = data_manager.get_delayed_flights_by_airport(airport)
        delayed_flights_data = [dict(row._mapping) for row in results]
        save_to_json_file(delayed_flights_data)  # Save to JSON file
        return jsonify(delayed_flights_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)