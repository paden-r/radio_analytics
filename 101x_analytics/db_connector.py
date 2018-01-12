import psycopg2
import os


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

    def disconnect(self):
        """
        Function that closes the DB connection
        :return:
        """
        self.cursor.close()
        self.connection.close()