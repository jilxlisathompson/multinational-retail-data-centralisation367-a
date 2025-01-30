import unittest
import pandas as pd
import numpy as np
from data_cleaning import DataCleaning  # Assuming your class is in data_cleaning.py

class TestDataCleaning(unittest.TestCase):
    def setUp(self):
        """Set up test environment and DataCleaning instance."""
        self.data_cleaner = DataCleaning(credential_file="login.yaml")

        # Sample test data
        self.test_data = pd.DataFrame({
            'user_id': [1, 2, 3, 4],
            'join_date': ['2023-01-01', 'NULL', '2023-03-15', '2023-04-01'],
            'name': ['Alice', 'NULL', 'Charlie', 'David']
        })

    def test_clean_user_data(self):
        """Test the clean_user_data method."""
        cleaned_data = self.data_cleaner.clean_user_data(self.test_data)

        # Verify "NULL" is replaced with NaN
        self.assertTrue(cleaned_data.isnull().sum().sum() == 0, "NULL values were not removed")

        # Verify rows with NaN values are dropped
        expected_rows = 2  # Only rows without "NULL" or NaN values should remain
        self.assertEqual(len(cleaned_data), expected_rows, "Unexpected row count after cleaning")

        # Verify "join_date" is converted to datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned_data['join_date']),
                        "join_date column is not converted to datetime")

    def test_row_count_validation(self):
        """Test that ValueError is raised if row count does not match 15284."""
        # Modify data to trigger the error
        self.test_data = self.test_data.iloc[:2]  # Fewer rows than expected
        
        with self.assertRaises(ValueError) as context:
            self.data_cleaner.clean_user_data(self.test_data)
        
        self.assertIn("Row count mismatch", str(context.exception))

if __name__ == "__main__":
    unittest.main()
