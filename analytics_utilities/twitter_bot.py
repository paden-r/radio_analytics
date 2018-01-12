import tweepy
import os


class TwitterBot(object):
    """
    Bot that tweets
    """
    def __init__(self):
        self.consumer_key = os.environ['TWITTER_CONSUMER_KEY']
        self.consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
        self.access_key = os.environ['TWITTER_ACCESS_KEY']
        self.access_secret = os.environ['TWITTER_ACCESS_SECRET']
        self.auth = tweepy.OAuthHandler(consumer_key=self.consumer_key, consumer_secret=self.consumer_secret)
        self.auth.set_access_token(key=self.access_key, secret=self.access_secret)
        self.bot = tweepy.API(self.auth)

    def tweet(self, message):
        """
        sends a tweet to twitter
        :param message: the test for the tweet
        :return:
        :type message: str
        """
        try:
            self.bot.update_status(message)
            return True, ""
        except tweepy.TweepError as error:
            return False, error

    def tweet_image(self, image_name, message):
        """
        tweets an image and message
        :param image_name: the file name of the image to be used
        :param message: the accompanied image
        :return:
        :type image_name: str
        :type message: str
        """
        try:
            self.bot.update_with_media(image_name, message)
            return True, ""
        except tweepy.TweepError as error:
            return False, error
