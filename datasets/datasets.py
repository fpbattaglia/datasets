import os.path
import configparser
import subprocess
import re
import socket
import tempfile
import shutil
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

        self.files = []
        self.dirs = []
        self.get_list_of_files()

    def get_list_of_files(self):
        """
        populate list of files and figure out what kind of dataset this is
        :return:
        None
        """
        command = [self.config['rsync_cmd'], self.config['rsync_list_opts'], self.source_location]

        print(' '.join(command))

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
            if len(item) > 1:  # TODO for the time being it does not do directories
                is_dir = item[0][0] == 'd'
                if item[-1][0] == '.':
                    pass
                elif is_dir:
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

        print(files)
        print(dirs)

        self.files = files
        self.dirs = dirs

    # noinspection PyMethodMayBeStatic
    def make_local_copy(self):
        """
        make local copy on disk of the entire dataset
        :return:
        success or fail
        """
        pass  # TODO make_local_copy

    def make_file_hashes(self):
        """
        make hashes of files that are present in the dataset at the moment
        :return:
        """
        pass  # TODO make_file_hashes

    def check_file_hashes(self):
        """
        checks
        :return:
        """

    def resync_to_source(self, cleanup=True):
        """

        :param cleanup: if True, removes the local copy on server
        :return:
        """
        pass  # TODO resync to source


class Probe:
    pass


class CNode(object):
    def __init__(self, **kwargs):
        self.name = socket.gethostname()
        self.config = cfg.copy()
        # read user defaults file
        self.config.update(parse_config('CNODE'))
        self.config.update(kwargs)
        self.directory = ''
        if 'dir_pattern' in self.config:
            self.base_dir = self.config['dir_pattern'].replace('%HOST', self.name)
        elif 'local_dir' in self.config:
            self.directory = self.config['local_dir']
        else:
            self.directory = self.base_dir

    def create_temp_directory(self, dataset=None):
        if dataset:
            root_dir = os.path.join(self.base_dir, dataset)
        else:
            root_dir = self.base_dir
        self.directory = tempfile.mkdtemp(dir=root_dir)

    def wipe_temp_directory(self):
        shutil.rmtree(self.directory)
        self.directory = ''


class ClusterOperation:
    pass
