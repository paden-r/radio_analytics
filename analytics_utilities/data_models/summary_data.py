import json


class SummaryData(object):
    """
    Class definition for summary data object
    """
    def __init__(self):
        self.summary_dict = {}
        self.most_common = None

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

    def get_max(self):
        """
        Function that calculates the most and least common artist in a given data set
        :return:
        """
        max_found = 0
        multiple_max = []
        for artist, count in self.summary_dict.items():
            if min == 0:
                self.most_common = artist
                max_found = count
                continue

            if count > max_found:
                max_found = count
                multiple_max = [artist]
                self.most_common = artist

            if count == max_found:
                if artist not in multiple_max:
                    multiple_max.append(artist)

        if len(multiple_max) > 1:
            self.most_common = multiple_max


