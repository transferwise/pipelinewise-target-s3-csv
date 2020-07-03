import json
import unittest
import simplejson

from nose.tools import assert_raises

import target_s3_csv
from target_s3_csv import s3


try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils


class TestIntegration(unittest.TestCase):
    """
    Integration Tests
    """
    maxDiff = None

    def setUp(self):
        self.config = test_utils.get_test_config()
        s3.setup_aws_client(self.config)

    def assert_three_streams_are_in_s3_bucket(self,
                                              should_metadata_columns_exist=False,
                                              should_hard_deleted_rows=False,
                                              compression=None,
                                              delimiter=',',
                                              quotechar='"'):
        """
        This is a helper assertion that checks if every data from the message-with-three-streams.json
        file is available in S3.
        Useful to check different loading methods (compressed, encrypted, custom delimiter and quotechar, etc.)
        without duplicating assertions
        """
        # TODO: This assertion function is currently a template and not implemented
        #       Here We should download files from S3 and compare to expected results based on the input
        #       parameters
        self.assertTrue(True)

    def persist_messages(self, messages):
        """Load data into S3"""
        target_s3_csv.persist_messages(messages, self.config)

    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with assert_raises(simplejson.scanner.JSONDecodeError):
            self.persist_messages(tap_lines)

    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-message-order.json')
        with assert_raises(Exception):
            self.persist_messages(tap_lines)

    def test_loading_csv_files(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        self.persist_messages(tap_lines)
        self.assert_three_streams_are_in_s3_bucket()

    def test_loading_csv_files_with_gzip_compression(self):
        """Loading multiple tables from the same input tap with gzip compression"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on gzip compression
        self.config['compression'] = 'gzip'
        self.persist_messages(tap_lines)
        self.assert_three_streams_are_in_s3_bucket(compression='gzip')

    def test_loading_csv_files_with_invalid_compression(self):
        """Loading multiple tables from the same input tap with invalid compression"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on a not supported compression method
        self.config['compression'] = 'INVALID_COMPRESSION_METHOD'

        # Invalid compression method should raise exception
        with assert_raises(NotImplementedError):
            self.persist_messages(tap_lines)

    
    def test_naming_convention(self):
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        self.config['naming_convention'] = "tester/{stream}/{timestamp}.csv"
        self.persist_messages(tap_lines)
        self.assert_three_streams_are_in_s3_bucket()
