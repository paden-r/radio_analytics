import datetime


class StatsAndGraph(object):
    """
    Class that holds methods to calculate stats on data and graph things
    """
    def __init__(self):
        pass

    @staticmethod
    def organize_data(data, logger=None):
        """
        Function that organizes the raw data gathered from the DB
        :param data: A dictionary of raw data {timestamp: {artist, title}}
        :type data: dict
        :param logger: A logger object
        :return:
        """
        if logger is None:
            logger = lambda x: print(x)
        daily_artist_count = {}
        daily_artist_song_count = {}
        song_hourly_count = {}
        for row in data:
            if isinstance(row, tuple):
                timestamp = row[0]
                # make the artist and title sql safe first
                artist = row[1].replace("'", "''")
                title = row[2].replace("'", "''")

                date, time = str(timestamp).split(' ')
                # populating data dicts, doing this is steps so I don't get confused looking back at this
                if date in daily_artist_song_count:
                    date_dict = daily_artist_count[date]
                    if artist in date_dict:
                        date_dict[artist] += 1
                    else:
                        date_dict[artist] = 1
                else:
                    daily_artist_count[date] = {}
                    daily_artist_count[date][artist] = 1

                if date in daily_artist_song_count:
                    date_dict = daily_artist_song_count[date]
                    if artist in date_dict:
                        song_dict = date_dict[artist]
                        if title in song_dict:
                            song_dict[title] += 1
                        else:
                            song_dict[title] = 1
                    else:
                        date_dict[artist] = {}
                        date_dict[artist][title] = 1
                else:
                    daily_artist_song_count[date] = {}
                    daily_artist_song_count[date][artist] = {}
                    daily_artist_song_count[date][artist][title] = 1

                # convert the time format to military and grab the hour
                military_hour = datetime.datetime.strptime(time, '%I:%M%p').strftime('%H')
                if date in song_hourly_count:
                    if military_hour in song_hourly_count[date]:
                        song_hourly_count[date][military_hour] += 1
                    else:
                        song_hourly_count[date][military_hour] = 1
                else:
                    song_hourly_count[date] = {}
                    song_hourly_count[date][military_hour] = 1
            else:
                logger('ERROR: Did not get the expected info from the DB breaking')
                logger(row)
                exit(-1)
        return daily_artist_count, daily_artist_song_count, song_hourly_count
