import unittest
from unittest.mock import patch

from nose.tools import assert_raises

import target_s3_csv

class TestUnit(unittest.TestCase):
    """
    Unit Tests
    """
    @classmethod
    def setUp(self):
        self.config = {}


    def test_config_validation(self):
        """Test configuration validator"""
        validator = target_s3_csv.utils.validate_config
        empty_config = {}
        minimal_config = {
            'aws_access_key_id':        "dummy-value",
            'aws_secret_access_key':    "dummy-value",
            's3_bucket':                "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors >= 0)
        self.assertGreater(len(validator(empty_config)),  0)

        # Minimal configuratino should pass - (nr_of_errors == 0)
        self.assertEqual(len(validator(minimal_config)), 0)


    def test_naming_convention_replaces_tokens(self):
        """Test that the naming_convention tokens are replaced"""
        message = {
            'stream': 'the_stream'
        }
        timestamp = 'fake_timestamp'
        s3_key = target_s3_csv.utils.get_target_key(message, timestamp=timestamp, naming_convention='test_{stream}_{timestamp}_test.csv')

        self.assertEqual('test_the_stream_fake_timestamp_test.csv', s3_key)


    def test_naming_convention_has_reasonable_default(self):
        """Test the default value of the naming convention"""
        message = {
            'stream': 'the_stream'
        }
        s3_key = target_s3_csv.utils.get_target_key(message)

        # default is "{stream}-{timestamp}.csv"
        self.assertTrue(s3_key.startswith('the_stream'))
        self.assertTrue(s3_key.endswith('.csv'))


    def test_naming_convention_honors_prefix(self):
        """Test that if the prefix is set in the config, that it is used in the s3 key"""
        message = {
            'stream': 'the_stream'
        }
        s3_key = target_s3_csv.utils.get_target_key(message, prefix='the_prefix__', naming_convention='folder1/test_{stream}_test.csv')

        self.assertEqual('folder1/the_prefix__test_the_stream_test.csv', s3_key)


    @patch("target_s3_csv.s3.boto3.session.Session.client")
    def test_create_client(self, mock_client):
        """Test that if an endpoint_url is provided in the config, that it is used in client request"""
        config = {
            'aws_access_key_id': 'foo',
            'aws_secret_access_key': 'bar',
            'aws_endpoint_url': 'other_url'
        }
        target_s3_csv.s3.create_client(config)
        mock_client.assert_called_with('s3', endpoint_url='other_url')
