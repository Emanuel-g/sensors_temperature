from flask import Flask, request, jsonify, make_response
from db_manager import DBManger
import sqlite3
# curl -H "Content-Type: application/json" -X POST http://localhost:8080/sensors -d "{\"sensor_id\":\"1\", \"temperature\":\"24.4\"}" - send data to server

# curl -H "Content-Type: application/json" -X GET http://localhost:8080/history -d "{\"sensor_id\":\"1\"} " - get information about specific sensor

# curl -H "Content-Type: application/json" -X GET http://localhost:8080/history -d "{\"sensor_id\":\"all\"}" - get information about all sensors


class Listener:
    def __init__(self) -> None:
        self.db_manager = DBManger()
        self.listener = Flask(__name__)
        self._set_flask_routes()

    def _set_flask_routes(self):
        self.listener.add_url_rule('/sensors', 'sensors', self._sensors_path, methods=['POST'])
        self.listener.add_url_rule('/history', 'history', self._history, methods=['GET'])

    def _sensors_path(self) -> request:
        """ Receives sensors data """
        res = request.get_json()
        try:
            self.db_manager.insert_sensors(res['sensor_id'], temperature=res['temperature'])
            return jsonify(f"Successfully added the temperature of sensor {res['sensor_id']}")
        except sqlite3.OperationalError:
            return make_response(jsonify("Wrong values"), 400)

    def _history(self) -> request:
        res = request.get_json()
        if res['sensor_id'] == 'all':
            return self.calculate_history(res['sensor_id'])
        elif self.db_manager.check_if_value_exists(res['sensor_id']):
            return self.calculate_history(res['sensor_id'])
        else:
            return jsonify(f"Sensor ID {res['sensor_id']} does not exist yet")

    def calculate_history(self, sensor_id):
        """ Calculates Metrics [min/max/avg]"""
        avg_value = self.db_manager.avg_sensor(sensor_id)
        min_value = self.db_manager.min_sensor(sensor_id)
        max_value = self.db_manager.max_sensor(sensor_id)
        result = {'AVG': avg_value,
                  'MIN': min_value,
                  'MAX': max_value}
        return jsonify(result)

    def run(self):
        """ Run flask """
        self.listener.run(host='0.0.0.0', port=8080, debug=True, threaded=True)


if __name__ == "__main__":
    listener = Listener()
    listener.run()
