"""
This module reads a CSV file from Sentiment140
(http://help.sentiment140.com/for-students/)
to create various leaderboards in Redis.
"""
import argparse
import enum
import logging

import pandas as pd
import redis

# Turn on logging with level of info
logging.basicConfig(format="%(asctime)s %(levelname)s - %(message)s",
                    level=logging.INFO)
_LOGGER = logging.getLogger("leaderboards")


class Polarity(enum.Enum):
    """
    Defines the meaning of the numeric values in the polarity column in the
    CSV.
    """
    negative = 0
    neutral = 2
    positive = 4


class Leaderboard:
    """
    Defines a leaderboard.
    """

    def __init__(self, name, size, client):
        """
        Creates a leaderboard instance.

        :param name: the name of the leaderboard
        :param size: the size of the leaderboard
        :param client: the Redis client used to save the data
        :type name: str
        :type size: int
        :type client: redis.client.StrictRedis
        """
        self.name = name
        self.size = size
        self.client = client

    def _top_n(self, key, rows):
        """
        Groups the rows by the incoming key and gets the top n.

        :param key: key to group the rows
        :param rows: the rows to group
        :type key: str
        :type rows: pandas.DataFrame
        :returns: the top n groups
        :rtype: pandas.DataFrame
        """
        groups = rows.groupby(key).size()
        groups = pd.DataFrame(groups.rename("counts"))
        groups = groups.sort_values("counts", ascending=False)
        groups = groups.head(self.size)
        groups = groups.reset_index()
        return groups

    def create(self, key, rows):
        """
        Creates a leaderboard with the incoming rows and saves into Redis.

        :param key: key to group the rows
        :param rows: the rows to group and save
        :type key: str
        :type rows: pandas.DataFrame
        """
        if rows.empty:
            data = pd.DataFrame([])
        else:
            data = self._top_n(key, rows)

        self.client.set(self.name, data.to_json(orient="records"))


def run(args):
    """
    Entry point to the script to generate the leaderboards in Redis.

    :param args: command line arguments provided to the script
    :type args: argparse.Namespace
    """
    source_data = pd.read_csv(args.csv, names=["polarity", "tweet_id",
                                               "tweet_date", "query",
                                               "twitter_id", "tweet"],
                              header=None)
    if source_data.empty:
        _LOGGER.error("No source data to generate leaderboards!")
        return

    try:
        # The .copy is used to remove SettingWithCopyWarning
        negative_tweets = source_data[source_data.polarity ==
                                      Polarity.negative.value].copy()
        positive_tweets = source_data[source_data.polarity ==
                                      Polarity.positive.value].copy()

        leaderboard_size = 10
        leaderboards = [("most_positive", "twitter_id", positive_tweets),
                        ("most_negative", "twitter_id", negative_tweets),
                        ("most_queried", "query", source_data),
                        ("most_tweeted", "twitter_id", source_data)]

        client = redis.StrictRedis.from_url(args.redis)
        # Throws a ConnectionRefusedError if it cannot connect
        client.ping()

        for name, key, rows in leaderboards:
            Leaderboard(name, leaderboard_size, client).create(key, rows)
    except redis.exceptions.ConnectionError:
        _LOGGER.exception("Failed to connect to Redis!")
    except AttributeError:
        _LOGGER.exception("Received an invalid file format!")


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument("--csv", required=True,
                        help="The CSV file to create leaderboards from.")
    PARSER.add_argument("--redis", required=True,
                        help="The Redis URL to save leaderboards.")
    run(PARSER.parse_args())
    _LOGGER.info("Finished")
