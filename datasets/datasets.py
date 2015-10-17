import os.path
import configparser
import subprocess
import re
import socket
import tempfile
import shutil
import warnings
import hashlib
import logging

from collections import defaultdict
from .datasets_conf_defaults import cfg

__author__ = 'fpbatta'


def parse_config(section):
    config = {}
    # read user defaults file
    home = os.path.expanduser('~')
    dconf = os.path.join(home, '.datasets', 'datasets.cfg')

    config_parser = configparser.ConfigParser()
    if os.path.isfile('datasets.cfg'):
        config_parser.read('datasets.cfg')
    elif os.path.isfile(dconf):
        config_parser.read(dconf)

    if section in config_parser.sections():
        config.update(config_parser[section])
    return config


# noinspection PyMethodMayBeStatic,PyMethodMayBeStatic
class Dataset(object):
    def __init__(self, dataset, **kwargs):
        self.config = cfg.copy()
        # read user defaults file

        self.config.update(parse_config('DATASETS'))
        self.config.update(kwargs)
        self.dataset = dataset
        if 'data_root' in self.config and not os.path.isabs(dataset):
            dataset = os.path.join(self.config['data_root'], dataset)

        if not os.path.isabs(dataset):
            raise ValueError('dataset ' + dataset + ' cannot be relative path, or data_root option not specified')

        self.source_dir = dataset

        if len(self.config['data_store']) > 0:
            self.source_location = self.config['data_store'] + ':' + self.source_dir
        else:
            self.source_location = self.source_dir

        if self.source_location[-1] != '/':
            self.source_location += '/'

        self.c_node = None
        self.local_copy_dir = None

        self.all_files = []
        self.dirs = []
        self.children = []
        self.get_list_of_files()

        self.files = defaultdict(list)
        for f in self.all_files:
            fp, ext = os.path.splitext(f)
            self.files[ext].append(f)

        self.has_local_copy = False
        self.hashes = {}

    def assign_cnode(self, **kwargs):
        if 'dir_pattern' in self.config:
            kwargs['dir_pattern'] = self.config['dir_pattern']
        if 'local_dir' in self.config:
            kwargs['local_dir'] = self.config['local_dir']

        self.c_node = CNode(**kwargs)

    def get_list_of_files(self):
        """
        populate list of files and figure out what kind of dataset this is
        :return:
        None
        """
        command = [self.config['rsync_cmd'], self.config['rsync_list_opts'], self.source_location]

        logging.debug('rsync command: ' + ' '.join(command))

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        out = out.decode('utf-8')

        if len(err) > 0:
            raise RuntimeError('rsync connection failed: ' + err.decode('utf-8'))
        file_lines = out.split('\n')
        files = []
        dirs = []
        for f in file_lines:
            item = f.split(' ')
            if len(item) > 1:
                is_dir = item[0][0] == 'd'
                if item[-1][0] == '.':
                    pass
                elif is_dir:
                    if self.config['subdirs_as_datasets']:
                        self.children.append(Dataset(os.path.join(self.dataset, item[-1]),
                                                     **self.config))
                    dirs.append(item[-1])
                else:
                    files.append(item[-1])

        def natural_sort_key(s, _nsre=re.compile('([0-9]+)')):
            return [int(text) if text.isdigit() else text.lower()
                    for text in re.split(_nsre, s)]

        files.sort(key=natural_sort_key)

        def extension_key(s):
            fp, ext = os.path.splitext(s)
            return ext

        files.sort(key=extension_key)

        logging.debug("files found: " + str(files))
        logging.debug("dirs found: " + str(dirs))

        self.all_files = files
        self.dirs = dirs

    def make_local_copy(self, extensions=None):
        """
        make local copy on disk of the entire dataset
        :return:
        None, it raises if there is an error
        """

        if not self.c_node:
            self.assign_cnode()

        loc_dir = self.c_node.create_temp_directory(dataset=self.dataset)

        command = [self.config['rsync_cmd'], self.config['rsync_sync_opts']]
        if extensions:
            for e in extensions:
                cmd = command.copy()
                cmd.append(os.path.join(self.source_location, '*.' + e))
                cmd.append(loc_dir)
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = p.communicate()
                if err:
                    raise RuntimeError('Rsync failed: ' + err.decode('utf-8'))
        elif self.config['subdirs_as_datasets']:  # don't copy subdirs
            for f in self.all_files:
                cmd = command.copy()
                cmd.append(os.path.join(self.source_location, f))
                cmd.append(loc_dir)
                logging.debug("rsync command: " + ' '.join(cmd))
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = p.communicate()
                if err:
                    raise RuntimeError('Rsync failed: ' + err.decode('utf-8'))
                logging.debug("rsync out: " + out.decode('utf-8'))

        else:
            command.append(self.source_location)
            command.append(loc_dir)
            p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            if err:
                raise RuntimeError('Rsync failed: ' + err.decode('utf-8'))

        self.has_local_copy = True
        return loc_dir

    def _make_file_hashes(self):
        """
        make hashes of files that are present in the dataset at the moment
        :return:
        """
        hashes = {}
        if not self.has_local_copy:
            warnings.warn("make_file_hashes: there's no local directory")
        loc_dir = self.c_node.directory
        for root, dirs, files in os.walk(loc_dir):
            for fn in files:
                hasher = hashlib.md5()
                with open(os.path.join(root, fn), "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hasher.update(chunk)
                hashes[os.path.relpath(os.path.join(root, fn), start=loc_dir)] = hasher.hexdigest()
        return hashes

    def create_file_hashes(self):
        self.hashes = self._make_file_hashes()

    def check_file_hashes(self):
        """
        checks file hashes
        :return: True if check succeeds False if it doesn't (and issues warning)
        """
        h = self._make_file_hashes()
        for (k, v) in iter(self.hashes.items()):
            if v != h[k]:
                warnings.warn("hashes not confirmed for file " + k)
                return False
        return True

    def resync_to_source(self, cleanup=True):
        """

        :param cleanup: if True, removes the local copy on server
        :return:
        """
        command = [self.config['rsync_command'], self.config['rsync_sync_opts'], self.c_node.directory,
                   self.source_location]
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if err:
            raise RuntimeError('Rsync failed: ' + err.decode('utf-8'))

        if cleanup:
            self.c_node.wipe_temp_directory()


class Probe:
    pass


class CNode(object):
    def __init__(self, **kwargs):
        """

        :param kwargs: dir_pattern: how to get the base dir (from host name etc.)
                       local_dir: specifying the local dir directly (alternative to dir_pattern)
        :return:
        """
        self.name = socket.gethostname()
        self.config = cfg.copy()
        # read user defaults file
        self.config.update(parse_config('CNODE'))
        self.config.update(kwargs)
        self.directory = ''
        if 'dir_pattern' in self.config:
            self.base_dir = self.config['dir_pattern'].replace('%HOST', self.name)
        elif 'local_dir' in self.config:
            self.base_dir = self.config['local_dir']
        else:
            raise ValueError('base_dir not specified')

    def create_temp_directory(self, dataset=None):
        root_dir = self.base_dir

        temp_dir = tempfile.mkdtemp(dir=root_dir)
        if dataset:
            self.directory = os.path.join(temp_dir, dataset)
            logging.debug("the temp dir is: " + temp_dir)
            logging.debug("the dataset is: " + dataset)
            logging.debug("the directory is: " + self.directory)
            os.mkdir(self.directory)
        else:
            self.directory = temp_dir

        if self.directory[-1] != '/':
            self.directory += '/'

        return self.directory

    def wipe_temp_directory(self):
        shutil.rmtree(self.directory)
        self.directory = ''


class ClusterOperation:
    pass
