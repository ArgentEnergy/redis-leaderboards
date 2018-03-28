"""This module is the unit test for the leaderboards module."""
import argparse
import unittest
from unittest.mock import patch

import pandas as pd
import redis

import leaderboards as lb


class MockRedis:
    """Mock class of Redis."""

    def __init__(self, ping_val):
        """
        Creates an instance of the Mock Redis class.

        :param ping_val: The ping value to determine if the server is running
        :type ping_val: str
        """
        self.ping_val = ping_val

    def ping(self):
        """
        Stub call for the Mocked Redis client.

        :returns: ping value
        :rtype: str
        """
        if self.ping_val != "PONG":
            raise redis.exceptions.ConnectionError()
        return self.ping_val

    def set(self, key, data):
        """
        Stub call for the Mocked Redis client.

        :param key: The Redis key
        :param data: The Redis data to set
        :type key: str
        :type data: str
        """
        pass


class TestLeaderboards(unittest.TestCase):
    """Unit Test case for the leaderboards module."""
    _csv_file = "/tmp/tmp.csv"
    _redis_url = "redis://localhost:6379/0"
    _csv_data = pd.DataFrame([{
        "polarity": "4",
        "tweet_id": "3",
        "tweet_date": "Mon May 11 03:17:40 UTC 2009",
        "query": "kindle2",
        "twitter_id": "tpryan",
        "tweet": """@stellargirl I loooooooovvvvvveee my Kindle2.
                                  Not that the DX is cool, but the 2 is
                                  fantastic in its own right."""
    }])

    def _assert_read_csv(self, mock):
        """
        Asserts the read CSV call.

        :param mock: The mocked CSV call
        :type mock: unittest.mock.MagicMock
        """
        cols = ["polarity", "tweet_id", "tweet_date",
                "query", "twitter_id", "tweet"]
        mock.assert_called_once_with(self._csv_file, names=cols, header=None)

    def _get_args(self, redis_url):
        """
        Gets the built arguments for the script call.

        :param redis_url: The Redis URL
        :type redis_url: str
        :returns: the built arguments
        :rtype: argparse.Namespace
        """
        args = argparse.Namespace()
        args.csv = self._csv_file
        args.redis = redis_url
        return args

    def _run_and_assert_csv(self, csv_data):
        """
        Runs and asserts the CSV call.

        :param csv_data: The CSV data to return with the mock call
        :type csv_data: pandas.DataFrame
        """
        with patch.object(pd, "read_csv", return_value=csv_data) as mock:
            lb.run(self._get_args(self._redis_url))
        self._assert_read_csv(mock)

    def _run_with_all_asserts(self, ping_val, args):
        """
        Runs and executes all asserts.

        :param ping_val: The ping value to determine if the server is running
        :param args: The script arguments
        :type ping_val: str
        :type args: argparse.Namespace
        """
        with patch.object(pd, "read_csv",
                          return_value=self._csv_data) as mock_csv:
            with patch.object(redis.StrictRedis, "from_url",
                              return_value=MockRedis(ping_val)) as mock_redis:
                lb.run(args)
            mock_redis.assert_called_once_with(args.redis)
        self._assert_read_csv(mock_csv)

    def test_empty_file(self):
        """Tests when the incoming CSV file is empty."""
        self._run_and_assert_csv(pd.DataFrame())

    def test_invalid_file_format(self):
        """Tests when the incoming CSV file has an invalid format."""
        invalid_file = pd.DataFrame([{
            "test": "1"
        }])
        self._run_and_assert_csv(invalid_file)

    def test_invalid_redis_url(self):
        """Tests when the incoming Redis URL is invalid."""
        self._run_with_all_asserts("", self._get_args("//bad-url"))

    def test_create_leaderboards(self):
        """Tests creating the leaderboards."""
        self._run_with_all_asserts("PONG", self._get_args(self._redis_url))


if __name__ == "__main__":
    unittest.main()
