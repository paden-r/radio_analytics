import sys
import pickle
import logging
import datetime
import os
from analytics_utilities.twitter_bot import TwitterBot as Tb
from analytics_utilities.screen_scraper import ScreenScrapper as Ss
from analytics_utilities.db_connector import PostgreSQL as Ps
from analytics_utilities.stats_and_graph import StatsAndGraph as SaG


class KROXAnalytics(object):
    def __init__(self):
        self.bot = None
        self.scrapper = None
        self.pickle_file_name = None
        self.db = None
        self.stat_and_graph = None
        self.logger = None
        self.log_directory = os.environ['LOG_DIRECTORY']
        self.build_logger()
        self.broadcast_info = None
        self.latest_timestamp = None
        self.questionable_flag = None

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

    def start_up(self, broadcast_url):
        """
        Function that is called first and gets the newest data
        :return:
        """
        self.bot = Tb()
        self.scrapper = Ss(broadcast_url)
        self.pickle_file_name = os.environ['PKL_FILE']
        self.db = Ps()
        try:
            last_run = pickle.load(open(self.pickle_file_name, 'rb'))
        except (OSError, IOError):
            last_run = {}

        if not last_run:
            self.latest_timestamp, self.broadcast_info = self.scrapper.parse_broadcast_page()
        else:
            self.latest_timestamp, self.broadcast_info = self.scrapper.parse_broadcast_page(
                time_threshold=last_run['time_stamp'])
        if self.latest_timestamp is None:
            self.log(['INFO', 'No new broadcast history', 'exiting'])
            exit(0)
        else:
            self.logger(self.broadcast_info)
        self.log(['INFO', 'Latest timestamp', str(self.latest_timestamp)])
        last_run['time_stamp'] = self.latest_timestamp
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
        questionable_decisions = []

        for timestamp, data in self.broadcast_info.items():
            if data['artist'] in flags:
                questionable_decisions.append({timestamp: data})

        if questionable_decisions:
            self.questionable_flag = questionable_decisions

    def shame(self, info):
        """
        Function that tweets out a questionable flag
        :param info: a dictionary of the questionable information
        :type info: dict
        :return:
        """
        for flag in info:
            for timestamp, data in flag.items():
                artist = data['artist']
                title = data['title']
                tweet_message = "@101x is making a questionable decision by playing \"{0}\" by {1} at {2} @JasonAndDeb".format(
                    title, artist, timestamp)
                success, error_message = self.bot.tweet(tweet_message)
                if not success:
                    self.logger(error_message)

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

    def get_broadcast_history(self, url):
        """
        Function that runs the analytics bot
        :return:
        """
        self.log(['INFO', 'Wake UP', 'GRABABRUSHANDPUTONALITTLEMAKEUP'])
        self.start_up(broadcast_url=url)
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

    def run_end_of_day_procedures(self):
        """
        Function that runs some basic analytics on the broadcast history for the last 24 hours after it categorized the
        day worth of history and stores it in the DB for later use.
        :return:
        """
        self.log(['INFO', 'Getting the broadcast history from the DB'])
        self.db = Ps()
        self.stat_and_graph = SaG()
        success, error = self.db.connect_to_db()
        if not success:
            self.log(['ERROR', 'DB error', error])
            exit(-1)

        broadcast_data = self.db.get_broadcast_history()
        summary, details, hourly = self.stat_and_graph.organize_data(broadcast_data)

        self.log(['INFO', 'Finished getting broadcast history and have organized it', 'sending to db'])
        self.db.insert_into_daily_summary(summary)
        self.log(['INFO', 'Inserted daily summary'])
        self.db.insert_into_daily_details(details)
        self.log(['INFO', 'Inserted daily details'])
        self.db.insert_hourly_counts(hourly)
        self.log(['INFO', 'Finished all db inserts', 'moving to clear the broadcast history table'])
        self.db.clear_broadcast_history()
        self.log(['INFO', 'End Of Day procedures have finished', 'shutting down'])
        self.db.disconnect()

    def run_end_of_week_analytics(self):
        """
        Function that gathers a weeks worth of data and does some basic analytics
        :return:
        """
        self.bot = Tb()
        self.stat_and_graph = SaG()
        self.db = Ps()
        delta = datetime.timedelta(days=7)
        start_date_obj = datetime.datetime.today() - delta
        start_date = start_date_obj.strftime('%m/%d/%Y')
        end_date = datetime.datetime.today().strftime('%m/%d/%Y')
        self.log(['INFO', 'Calculated start date and end date for the week', start_date, end_date])
        tables_to_query = ['daily_summary', 'daily_details', 'hourly_count']
        # weekly_data = [summary, details, hourly]
        weekly_data = self.db.get_weekly_data(tables=tables_to_query, start=start_date, end=end_date)

    def run_end_of_month_analytics(self):
        """
        Function that gathers calculates some basic analytics for the last month of broadcast history
        :return:
        """
        pass


if __name__ == '__main__':
    if sys.argv[1] in ['eod', "eom", 'url']:
        if sys.argv[1] == 'url':
            if len(sys.argv) == 3:
                analytics = KROXAnalytics()
                analytics.get_broadcast_history(url=sys.argv[2])
            else:
                print("Unknown parameters given")
                exit(-1)
        if sys.argv[1] == 'eod':
            analytics = KROXAnalytics()
            analytics.run_end_of_day_procedures()
        if sys.argv[1] == 'eom':
            analytics = KROXAnalytics()
            analytics.run_end_of_month_analytics()
    else:
        print("Unknown parameters given")
        exit(-1)


