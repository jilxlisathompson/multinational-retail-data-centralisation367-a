import unittest
from data_extraction import DataExtractor

class TestDataExtractor(unittest.TestCase):
    def setUp(self):
        # This method runs before every test. Use it to set up the test environment.
        self.extractor = DataExtractor(credential_file="path/to/credentials.json")

    def test_read_rds_table(self):
        # Test the read_rds_table method
        df = self.extractor.read_rds_table('existing_table_name')
        self.assertGreater(len(df), 0)  # Check that the DataFrame is not empty

    # Add more test methods for other functionalities of the DataExtractor class

if __name__ == '__main__':
    unittest.main()
