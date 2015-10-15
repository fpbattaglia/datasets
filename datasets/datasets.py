import os.path
import configparser
import subprocess
from .datasets_conf_defaults import cfg

__author__ = 'fpbatta'


class Dataset:
    def __init__(self, dataset, **kwargs):
        self.config = cfg.copy()
        # read user defaults file
        home = os.path.expanduser('~')
        dconf = os.path.join(home, '.datasets', 'datasets.cfg')
        config_parser = configparser.ConfigParser()
        if os.path.isfile('datasets.cfg'):
            config_parser.read('datasets.cfg')
        elif os.path.isfile(dconf):
            config_parser.read(dconf)

        if 'DATASETS' in config_parser.sections():
            self.config.update(config_parser['DATASETS'])
        self.config.update(kwargs)

        if 'data_root' in self.config and not os.path.isabs(dataset):
            dataset = os.path.join(self.config['data_root'], dataset)

        if not os.path.isabs(dataset):
            raise ValueError('dataset cannot be relative path, or data_root option not specified')

        self.source_dir = dataset

        self.c_node = None
        self.local_copy_dir = None

        self.files = []
        self.get_list_of_files()

    def get_list_of_files(self):
        """
        populate list of files and figure out what kind of dataset this is
        :return:
        None
        """
        command = [self.config'rsync_cmd'], self.config['rsync_list_opts']]

        location = self.config['data_store'] + self.source_dir
        if location[-1] != '/':
            location += '/'

        command.append(location)

        p = subprocess.Popen(command, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)

        pass  # TODO get_list_of_files

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
        pass # TODO make_file_hashes

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


class CNode:
    pass


class ClusterOperation:
    pass
