
import unittest
import sys
import os.path
import shutil
import re
import hashlib
import logging
from datasets.datasets import Dataset, CNode

__author__ = 'fpbatta'

sys.path.append('/Users/fpbatta/src')


class TestDatasetLocalOEData(unittest.TestCase):
    def test_list(self):
        d = Dataset('2014-10-30_16-07-29', data_root='/Users/fpbatta/dataTestRonny',
                    data_store='')
        exp_config = {'data_store': '', 'rsync_sync_opts': '-avz',
                      'data_root': '/Users/fpbatta/dataTestRonny', 'rsync_list_opts': '--list-only',
                      'rsync_cmd': 'rsync', 'subdirs_as_datasets': False}
        self.assertEqual(d.config, exp_config)

    def test_cnode(self):
        c = CNode(dir_pattern='/peones/%HOST')
        self.assertEqual(c.base_dir, '/peones/Francescos-MacBook-Pro.local')

    def test_cnode_directory(self):
        c = CNode(dir_pattern='/Users/fpbatta/local_store/%HOST')
        temp_dir = c.create_temp_directory(dataset='2014-10-30_16-07-29')
        self.assertTrue(
            re.match(
                '/Users/fpbatta/local_store/Francescos-MacBook-Pro.local/.+/2014-10-30_16-07-29/',
                temp_dir))
        self.assertTrue(os.path.isdir(temp_dir))


fd1_dat =  \
    """ciccio
peppe
strano
"""

fd2_dat = \
    """non
fiore
bello
"""

fd3_dat = \
    """1
2
3
4
5
6
7
7
8
"""

a1_avi = \
    """1 2
3 4
5 6
7 8
"""

sfd1_dat = \
    """cucu
ciao
bella
"""

sfd2_dat = \
    """you do whatever you want
and why
"""


class TestDatasetLocalFakeData(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        home = os.path.expanduser('~')
        self.data_root = os.path.join(home, 'fake_test_data')
        self.s_dir = os.path.join(self.data_root, 'fake_dataset')
        self.temp_dirs = []
        shutil.rmtree(self.s_dir, ignore_errors=True)
        os.makedirs(self.s_dir)
        with open(os.path.join(self.s_dir, 'fd1.dat'), 'w') as f:
            f.write(fd1_dat)
        with open(os.path.join(self.s_dir, 'fd2.dat'), 'w') as f:
            f.write(fd2_dat)
        with open(os.path.join(self.s_dir, 'fd3.dat'), 'w') as f:
            f.write(fd3_dat)
        with open(os.path.join(self.s_dir, 'a1.avi'), 'w') as f:
            f.write(a1_avi)
        os.makedirs(os.path.join(self.s_dir, 'the_subdir'))
        with open(os.path.join(self.s_dir, 'the_subdir', 'sfd1.dat'), 'w') as f:
            f.write(sfd1_dat)
        with open(os.path.join(self.s_dir, 'the_subdir', 'sfd2.dat'), 'w') as f:
            f.write(sfd2_dat)

    def test_list(self):
        d = Dataset('fake_dataset', data_store='', data_root=self.data_root)
        exp_config = {'rsync_sync_opts': '-avz', 'data_root': '/Users/fpbatta/fake_test_data',
                      'rsync_list_opts': '--list-only', 'data_store': '',
                      'rsync_cmd': 'rsync', 'subdirs_as_datasets': False
                      }
        self.assertEqual(d.config, exp_config)
        exp_files = {'.dat': ['fd1.dat', 'fd2.dat', 'fd3.dat'], '.avi': ['a1.avi']}
        self.assertEqual(d.files, exp_files)
        exp_all_files = ['a1.avi', 'fd1.dat', 'fd2.dat', 'fd3.dat']
        self.assertEqual(d.all_files, exp_all_files)

    def test_temp_copy(self):
        d = Dataset('fake_dataset', data_store='', data_root=self.data_root,
                    local_dir='/Users/fpbatta/local_store')
        temp_dir = d.make_local_copy()
        self.temp_dirs.append(temp_dir)
        self.assertEqual(os.listdir(temp_dir), ['a1.avi', 'fd1.dat', 'fd2.dat', 'fd3.dat',
                                                'the_subdir'])

    def test_subdirs_empty(self):
        d = Dataset('fake_dataset', data_store='', data_root=self.data_root,
                    local_dir='/Users/fpbatta/local_store')
        self.assertFalse(d.children)

    def test_subdirs_temp_copy(self):
        d = Dataset('fake_dataset', data_store='', data_root=self.data_root,
                    local_dir='/Users/fpbatta/local_store')
        temp_dir = d.make_local_copy()
        self.temp_dirs.append(temp_dir)
        self.assertEqual(os.listdir(temp_dir), ['a1.avi', 'fd1.dat', 'fd2.dat', 'fd3.dat',
                                                'the_subdir'])
        self.assertTrue(os.path.isdir(os.path.join(temp_dir, 'the_subdir')))
        logging.debug(os.listdir(os.path.join(temp_dir, 'the_subdir')))

    def test_subdirs(self):
        d = Dataset('fake_dataset', data_store='', data_root=self.data_root,
                    local_dir='/Users/fpbatta/local_store', subdirs_as_datasets=True)
        self.assertTrue(d.children)
        temp_dir = d.make_local_copy()
        self.temp_dirs.append(temp_dir)
        self.assertEqual(os.listdir(temp_dir), ['a1.avi', 'fd1.dat', 'fd2.dat', 'fd3.dat'])

    def test_hashes(self):
        d = Dataset('fake_dataset', data_store='', data_root=self.data_root,
                    local_dir='/Users/fpbatta/local_store')
        temp_dir = d.make_local_copy()
        self.temp_dirs.append(temp_dir)
        d.create_file_hashes()
        self.assertTrue(d.hashes)
        h = hashlib.md5()
        h.update(fd1_dat.encode('utf-8'))
        self.assertEqual(h.hexdigest(), d.hashes['fd1.dat'])

        h = hashlib.md5()
        h.update(sfd1_dat.encode('utf-8'))
        self.assertEqual(h.hexdigest(), d.hashes['the_subdir/sfd1.dat'])

        logging.debug("hashes: " + str(d.hashes))

    def test_check_hashes(self):
        d = Dataset('fake_dataset', data_store='', data_root=self.data_root,
                    local_dir='/Users/fpbatta/local_store')
        temp_dir = d.make_local_copy()
        self.temp_dirs.append(temp_dir)
        d.create_file_hashes()
        self.assertTrue(d.check_file_hashes())

    def tearDown(self):
        shutil.rmtree(self.s_dir, ignore_errors=True)
        for d in self.temp_dirs:
            dd = os.path.dirname(os.path.dirname(d))
            logging.debug("destroying folder: " + dd)
            shutil.rmtree(dd)


if __name__ == '__main__':
    unittest.main()
