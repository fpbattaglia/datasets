__author__ = 'fpbatta'

import unittest
import sys

sys.path.append('/Users/fpbatta/src')

from datasets.datasets import Dataset, CNode

class TestDatasetLocalOpen(unittest.TestCase):
    def test_list(self):
        d = Dataset('2014-10-30_16-07-29', data_root='/Users/fpbatta/dataTestRonny',
                    data_store='')
        self.assertTrue(True)

    def test_cnode(self):
        c = CNode(dir_pattern='/peones/%HOST')
        self.assertEqual(c.base_dir, '/peones/Francescos-MacBook-Pro.local')

if __name__ == '__main__':
    unittest.main()