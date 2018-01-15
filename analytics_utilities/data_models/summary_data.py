import json


class SummaryData(object):
    """
    Class definition for summary data object
    """
    def __init__(self):
        self.summary_dict = {}
        self.most_common = None
        self.least_common = None

    def data_intake(self, intake_data):
        """
        Function that intakes data to adds it to its overall totals
        :param data_dict: dictionary of data that comes in as a string
        :type data_dict: str
        :return:
        """
        data_dict = json.loads(intake_data)
        for artist, count in data_dict.items():
            if artist not in self.summary_dict:
                self.summary_dict[artist] = int(count)
            else:
                self.summary_dict[artist] += int(count)

    def get_min_and_max(self):
        """
        Function that calculates the most and least common artist in a given data set
        :return:
        """
        min = 0
        max = 0
        multiple_max = []
        multiple_min = []
        for artist, count in self.summary_dict.items():
            if min == 0:
                self.most_common = artist
                self.least_common = artist
                min = count
                max = count
                continue

            if count > max:
                max = count
                multiple_max = [artist]
                self.most_common = artist

            if count == max:
                if artist not in multiple_max:
                    multiple_max.append(artist)

            if count < min:
                min = count
                multiple_min = [artist]
                self.least_common = artist

            if count == min:
                if artist not in multiple_min:
                    multiple_min.append(artist)

        if len(multiple_max) > 1:
            self.most_common = multiple_max

        if len(multiple_min) > 1:
            self.least_common = multiple_min

