import json


class DetailData(object):
    """
    Class definition for summary data object
    """
    def __init__(self):
        self.details_dict = {}
        self.most_common = None
        self.second_most = None
        self.third_most = None
        self.data_count = 0

    def data_intake(self, intake_data):
        """
        Function that intakes data to adds it to its overall totals
        :param intake_data: dictionary of data that comes in as a string
        :type intake_data: str
        :return:
        """
        data_dict = json.loads(intake_data)
        self.data_count += 1
        for artist, song_dict in data_dict.items():
            if artist not in self.details_dict:
                self.details_dict[artist] = song_dict
            else:
                for song in song_dict:
                    if song not in self.details_dict[artist]:
                        self.details_dict[artist][song] = song_dict[song]

    def get_max(self):
        """
        Function that calculates the most and least common artist in a given data set
        :return:
        """
        max_found = 0
        second_max = 0
        third_max = 0
        multiple_max = []
        multiple_second = []
        multiple_third = []
        for artist, songs in self.details_dict.items():
            count = len(songs)
            if count > max_found:
                max_found = count
                multiple_max = [artist]
                self.most_common = artist

            elif count == max_found:
                if artist not in multiple_max:
                    multiple_max.append(artist)

            elif count > second_max:
                second_max = count
                multiple_second = [artist]
                self.second_most = artist

            elif count == second_max:
                if artist not in multiple_second:
                    multiple_second.append(artist)

            elif count > third_max:
                third_max = count
                multiple_third = [artist]
                self.third_most = artist

            elif count == third_max:
                if artist not in multiple_third:
                    multiple_third.append(artist)

        if len(multiple_max) > 1:
            self.most_common = multiple_max

        if len(multiple_second) > 1:
            self.second_most = multiple_second

        if len(multiple_third) > 1:
            self.third_most = multiple_third
