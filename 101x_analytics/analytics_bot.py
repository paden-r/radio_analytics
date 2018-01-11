import pickle
import logging
import datetime
import os
from .twitter_bot import TwitterBot as Tb
from .screen_scraper import ScreenScrapper as Ss
from .db_connector import PostgreSQL as Ps


class KROX_Analytics(object):
    def __init__(self):
        self.bot = Tb()
        self.scrapper = Ss("http://www.101x.com/broadcasthistory")
        self.pickle_file_name = "last_run.pkl"
        self.db = Ps()
        self.logger = None
        self.build_logger()
        self.broadcast_info = None
        self.ordered_list_of_timestamps = None
        self.questionable_flag = None
        self.log_directory = os.environ['LOG_DIRECTORY']

    def build_logger(self):
        """
        Build a logger
        :return:
        """
        name = 'analytics'
        logging_level = logging.DEBUG
        logger = logging.getLogger(name)
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        file_handler = logging.FileHandler('{}/{}_{}.log'.format(self.log_directory, name, today))
        formatter = logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.setLevel(logging_level)
        self.logger = logger.debug

    def log(self, message):
        """
        formats the log message
        :param message: a list of strings to log out
        :type message: list
        """
        log_message = '\t'.join(message)
        self.logger(log_message)

    def start_up(self):
        """
        Function that is called first and gets the newest data
        :return:
        """
        try:
            last_run = pickle.load(open(self.pickle_file_name, 'rb'))
        except (OSError, IOError):
            last_run = {}

        if not last_run:
            self.ordered_list_of_timestamps, self.broadcast_info = self.scrapper.parse_broadcast_page()
        else:
            self.ordered_list_of_timestamps, self.broadcast_info = self.scrapper.parse_broadcast_page(time_threshold=last_run['time_stamp'])
        if len(self.ordered_list_of_timestamps) == 0:
            self.log(['INFO', 'No new boradcast history', 'exiting'])
            exit(0)
        else:
            self.logger(self.broadcast_info)
        latest_timestamp = self.ordered_list_of_timestamps[0]
        self.log(['INFO', 'Latest timestamp', str(latest_timestamp)])
        last_run['time_stamp'] = latest_timestamp
        pickle.dump(last_run, open(self.pickle_file_name, 'wb'))
        self.log(['INFO', 'Got broadcast history'])

    def analyze_broadcast_history(self):
        """
        Function that will go through the broadcast history and look for flags to alert on
        :return:
        """
        success, error = self.db.connect_to_db()
        if not success:
            self.log(['ERROR', 'DB error', error])
            exit(-1)

        flags_tuples = self.db.grab_questionable_decisions()
        flags = []
        for obj in flags_tuples:
            if isinstance(obj, tuple):
                flags.append(obj[0])
            elif isinstance(obj, str):
                flags.append(obj)
        questionalbe_decisions = []

        for timestamp, data in self.broadcast_info.items():
            if data['artist'] in flags:
                questionalbe_decisions.append({timestamp: data})

        if questionalbe_decisions:
            self.questionable_flag = questionalbe_decisions

    def shame(self, info):
        """
        Function that tweets out a questionable flag
        :param info: a dictionary of the questionable information
        :type info: dict
        :return:
        """
        for timestamp, data in info.items():
            artist = data['artist']
            title = data['title']
            tweet_message = "@JasonAndDeb @101x is making another questionable \
                            decision by playing {0} by {1} at {2}".format(title, artist, timestamp)
            self.bot.tweet(tweet_message)

    def input_broadcast_history(self):
        """
        Function that inserts the scrapped broadcast history into the DB
        :return:
        """
        today = datetime.datetime.today().strftime('%m/%d/%Y')
        for timestamp, data in self.broadcast_info.items():
            full_timestamp = "{} {}".format(today, timestamp)
            insert_data = (full_timestamp, data['artist'], data['title'])
            self.db.insert_into_broadcast_history(insert_data)

    def run(self):
        """
        Function that runs the analytics bot
        :return:
        """
        self.log(['INFO', 'Wake UP', 'GRABABRUSHANDPUTONALITTLEMAKEUP'])
        self.start_up()
        self.log(['INFO', 'Analyzing'])
        self.analyze_broadcast_history()

        if self.questionable_flag is not None:
            self.log(['INFO', 'SHAMING'])
            self.logger(self.questionable_flag)
            self.shame(self.questionable_flag)

        self.log(['INFO', 'storing latest broadcast history'])
        self.input_broadcast_history()
        self.log(['INFO', 'shutting down'])
        self.db.disconnect()


if __name__ == '__main__':
    analytics = KROX_Analytics()
    analytics.run()


