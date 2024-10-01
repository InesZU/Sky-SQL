import sys
from sqlalchemy import create_engine, text


class FlightData:
    """
    The FlightData class is a Data Access Layer (DAL) object that provides an
    interface to the flight data in the SQLITE database. When the object is created,
    the class forms connection to the sqlite database file.
    """
    def __init__(self, db_uri):
        """
        Initialize the connection to the database.
        """
        self.engine = create_engine(db_uri)
        self.db_uri = db_uri

    def _execute_query(self, query, params):
        """
        Helper method to execute queries with the given parameters and return all results.
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query), params)
            return [row for row in result]

    def get_flight_by_id(self, flight_id):
        """
        Fetch flight details by flight ID.
        """
        QUERY_FLIGHT_BY_ID = """
        SELECT flights.ID, flights.ORIGIN_AIRPORT, flights.DESTINATION_AIRPORT, 
               flights.DEPARTURE_DELAY AS DELAY, airlines.AIRLINE
        FROM flights
        JOIN airlines ON flights.AIRLINE = airlines.ID
        WHERE flights.ID = :id
        """
        params = {'id': flight_id}
        return self._execute_query(QUERY_FLIGHT_BY_ID, params)

    def get_flights_by_date(self, day, month, year):
        """
        Fetch all flights on a specific date, including airline names.
        """
        QUERY_FLIGHTS_BY_DATE = """
        SELECT flights.ID, flights.ORIGIN_AIRPORT, flights.DESTINATION_AIRPORT, 
               flights.DEPARTURE_DELAY AS DELAY, airlines.AIRLINE
        FROM flights
        JOIN airlines ON flights.AIRLINE = airlines.ID
        WHERE flights.DAY = :day
        AND flights.MONTH = :month
        AND flights.YEAR = :year
        """
        params = {'day': day, 'month': month, 'year': year}
        return self._execute_query(QUERY_FLIGHTS_BY_DATE, params)

    def get_delayed_flights_by_airline(self, airline_name):
        """
        Fetch delayed flights by airline.
        """
        QUERY_DELAYED_FLIGHTS_BY_AIRLINE = """
        SELECT flights.ID, flights.ORIGIN_AIRPORT, flights.DESTINATION_AIRPORT, 
               flights.DEPARTURE_DELAY AS DELAY, airlines.AIRLINE
        FROM flights
        JOIN airlines ON flights.AIRLINE = airlines.ID
        WHERE airlines.AIRLINE LIKE :airline_name AND flights.DEPARTURE_DELAY > 20
        """
        params = {'airline_name': f'%{airline_name}%'}
        return self._execute_query(QUERY_DELAYED_FLIGHTS_BY_AIRLINE, params)

    def get_delayed_flights_by_airport(self, airport_code):
        """
        Fetch delayed flights by origin airport.
        """
        QUERY_DELAYED_FLIGHTS_BY_AIRPORT = """
        SELECT flights.ID, flights.ORIGIN_AIRPORT, flights.DESTINATION_AIRPORT, 
               flights.DEPARTURE_DELAY AS DELAY, airlines.AIRLINE
        FROM flights
        JOIN airlines ON flights.AIRLINE = airlines.ID
        WHERE flights.ORIGIN_AIRPORT = :IATA_code AND flights.DEPARTURE_DELAY > 20
        AND flights.DEPARTURE_DELAY > 0
        """
        params = {'IATA_code': airport_code}
        return self._execute_query(QUERY_DELAYED_FLIGHTS_BY_AIRPORT, params)

    def quit(self):
        """
        Exits the program.
        """
        print("Exiting the program.")
        sys.exit()
