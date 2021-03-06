import requests
from bs4 import BeautifulSoup as Bs


class ScreenScrapper(object):
    """
    A class for the screen scrapper functions for the 101x analytics bot
    """

    def __init__(self, url):
        self.page = requests.get(url)
        self.soup = Bs(self.page.text, 'html.parser')

    def parse_broadcast_page(self, time_threshold=None):
        """
        function that parses the page for broadcast history
        :return: A dictionary of boradcast history
        """
        views_rows = self.soup.find_all(class_='views-row')
        broadcast_data = {}
        first_time_stamp = None
        for row in views_rows:
            try:
                time_stamp = str(row.find(class_='views-field-field-timestamp').find('span').string)
                if time_stamp == time_threshold:
                    break
                artist = str(row.find(class_='views-field-field-artist').find('span').string)
                title = str(row.find(class_='views-field-field-title').find('div').string)
                broadcast_data[time_stamp] = {'title': title, 'artist': artist}
                if first_time_stamp is None:
                    first_time_stamp = time_stamp
            except AttributeError:
                break

        return first_time_stamp, broadcast_data


if __name__ == "__main__":
    ss = ScreenScrapper("http://www.101x.com/broadcasthistory")
    ordered, data = ss.parse_broadcast_page()
    print(ordered)
