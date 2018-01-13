import psycopg2
import os
import json


class PostgreSQL(object):
    """
    class for getting and inserting data into the analytics db
    """

    def __init__(self):
        self.db_name = os.environ['PSQL_DB_NAME']
        self.db_host = os.environ['PSQL_HOST']
        self.db_user = os.environ['PSQL_USER']
        self.db_userpw = os.environ['PSQL_PW']
        self.connection_string = "dbname='{0}' user='{1}' host='{2}' password='{3}'".format(self.db_name, self.db_user,
                                                                                            self.db_host,
                                                                                            self.db_userpw)
        self.connection = None
        self.cursor = None

    def connect_to_db(self):
        """
        Connects to the DB or returns an error if a connection error occurs
        :return: boolean true or false with error string
        """
        try:
            self.connection = psycopg2.connect(self.connection_string)
            self.cursor = self.connection.cursor()
        except Exception as error:
            return False, "A connection exception occurred: {}".format(error)

        return True, ""

    def grab_questionable_decisions(self):
        """
        Grabs the rows in the questionable_flags table in the post gres db
        :return:
        """
        assert(self.cursor, psycopg2)
        sql_string = "SELECT * FROM questionable_flags;"
        self.cursor.execute(sql_string)
        rows = self.cursor.fetchall()

        return rows

    def insert_into_broadcast_history(self, data):
        """
        Inserts a row into the boradcast_history table
        :param data: the data to be substituted in the query string
        :type data: tuple
        :return:
        """
        query = "INSERT INTO broadcast_history (timestamp, artist, title) \
                 VALUES ('{}', '{}', '{}');".format(data[0], data[1].replace("'", "''"), data[2].replace("'", "''"))
        self.cursor.execute(query, data)
        self.connection.commit()

    def get_broadcast_history(self):
        """
        Dumps out all of the broadcast_history table
        :return:
        """
        query = "SELECT * FROM broadcast_history;"
        self.cursor.execute(query)
        rows_returned = self.cursor.fetchall()
        return rows_returned

    def insert_into_daily_summary(self, data):
        """
        Inserts a row into the daily_summary table
        :param data: dictionary of data data['timestamp'] = {artist totals}
        :type data: dict
        :return:
        """
        for time_stamp, summary_dict in data.items():
            string_summary = json.dumps(summary_dict)
            query = "INSERT INTO daily_summary (timestamp, summary) \
                     VALUES ('{}', '{}');".format(time_stamp, string_summary)

            self.cursor.execute(query, data)
            self.connection.commit()

    def insert_into_daily_details(self, data):
        """
        Inserts a row into the daily_detail table
        :param data: dictionary of data data['timestamp'] = {artist: {song totals}}
        :type data: dict
        :return:
        """
        for time_stamp, details_dict in data.items():
            string_details = json.dumps(summary_dict)
            query = "INSERT INTO daily_details (timestamp, details) \
                     VALUES ('{}', '{}');".format(time_stamp, string_details)

            self.cursor.execute(query, data)
            self.connection.commit()

    def insert_hourly_counts(self, data):
        """
        Inserts a row into the hourly_count table
        :param data: dictionary of data data['timestamp'] = {00: 6, 01: 8 ... }
        :type data: dict
        :return:
        """
        for time_stamp, time_totals in data.items():
            string_totals = json.dumps(time_totals)
            query = "INSERT INTO hourly_count (timestamp, count) \
                     VALUES ('{}', '{}');".format(time_stamp, string_totals)

            self.cursor.execute(query, data)
            self.connection.commit()

    def clear_broadcast_history(self):
        """
        Function that will clean out the broadcast history table after the data has been reorganize into a useful format
        :return:
        """
        pass

    def disconnect(self):
        """
        Function that closes the DB connection
        :return:
        """
        self.cursor.close()
        self.connection.close()
