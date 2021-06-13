import sqlite3
from datetime import datetime
from threading import Lock

db_file = "data.db"
lock = Lock()


class DBManger:
    def __init__(self) -> None:
        self.create_db()

    def create_db(self):
        query = 'CREATE TABLE IF NOT EXISTS Sensors (date int, sensor_id int, temperature float)'
        self.__run_sql_command(query)

    def insert_sensors(self, sensor_id: str, temperature: str):
        query = f"insert into Sensors values ({int((datetime.now().strftime('%Y%m%d')))},{sensor_id}, {temperature})"
        self.__run_sql_command(query)

    @staticmethod
    def __run_sql_command(query: str, fetch_all=True) -> list:
        with lock:
            connection = sqlite3.connect(db_file, check_same_thread=False)
            cur = connection.cursor()
            results = cur.execute(query)
            connection.commit()
            if fetch_all:
                data = results.fetchall()
            else:
                data = results.fetchone()
            connection.close()
        return data

    def check_if_value_exists(self, sensor_id: str) -> list:
        query = f"SELECT EXISTS(SELECT 1 FROM Sensors WHERE sensor_id ={sensor_id})"
        exists = self.__run_sql_command(query, fetch_all=False)
        return exists[0]

    def avg_sensor(self, sensor_id: str) -> int or float:
        if sensor_id == 'all':
            query = 'SELECT AVG(temperature) FROM Sensors'
        else:
            query = f'SELECT AVG(temperature) FROM Sensors WHERE sensor_id == {sensor_id} '
        avg_value = self.__run_sql_command(query)
        return avg_value[0][0]

    def min_sensor(self, sensor_id: str) -> int or float:
        if sensor_id == 'all':
            query = 'SELECT MIN(temperature) FROM Sensors'
        else:
            query = f'SELECT MIN(temperature) FROM Sensors WHERE sensor_id == {sensor_id} '
        min_value = self.__run_sql_command(query)
        return min_value[0][0]

    def max_sensor(self, sensor_id: str) -> int or float:
        if sensor_id == 'all':
            query = 'SELECT MAX(temperature) FROM Sensors'
        else:
            query = f'SELECT MAX(temperature) FROM Sensors WHERE sensor_id == {sensor_id} '
        max_value = self.__run_sql_command(query)
        return max_value[0][0]

