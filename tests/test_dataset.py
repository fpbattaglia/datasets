__author__ = 'fpbatta'

import unittest
import sys

sys.path.append('/Users/fpbatta/src')

from datasets.datasets import Dataset

class TestDatasetOpen(unittest.TestCase):
    def test_list(self):
        d = Dataset('2014-10-30_16-07-29', data_root='/volume1/homes/fpbatta/dataTestRonny')
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()