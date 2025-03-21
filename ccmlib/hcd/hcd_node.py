# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# DataStax Hyper-Converged Database (HCD) node

from __future__ import absolute_import, with_statement

import os
import re
import subprocess
import shutil
import tarfile
import tempfile
import yaml

from distutils.version import LooseVersion
from six.moves import urllib, xrange

from ccmlib import common, node, repository
from ccmlib.common import ArgumentError, rmdirs
from ccmlib.node import Node



HCD_ARCHIVE = "https://downloads.datastax.com/hcd/hcd-%s-bin.tar.gz"

class HcdNode(Node):

    """
    Provides interactions to a HCD node.
    """

    @staticmethod
    def get_version_from_build(install_dir=None, node_path=None, cassandra=False):
        if install_dir is None and node_path is not None:
            install_dir = node.get_install_dir_from_cluster_conf(node_path)
        if install_dir is not None:
            # Binary cassandra installs will have a 0.version.txt file
            version_file = os.path.join(install_dir, '0.version.txt')
            if os.path.exists(version_file):
                with open(version_file) as f:
                    return LooseVersion(f.read().strip())
            # For HCD look for a hcd*.jar and extract the version number
            hcd_version = get_hcd_version(install_dir)
            if (hcd_version is not None):
                if cassandra:
                    return get_hcd_cassandra_version(install_dir)
                else:
                    return LooseVersion(hcd_version)
            # Source cassandra installs we can read from build.xml
            return Node.get_version_from_build(install_dir, cassandra)
        raise common.CCMError("Cannot find version")


    def __init__(self, name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None, byteman_port='0', environment_variables=None, derived_cassandra_version=None, **kwargs):
        super(HcdNode, self).__init__(name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface, byteman_port, environment_variables=environment_variables, derived_cassandra_version=derived_cassandra_version, **kwargs)

    def get_install_cassandra_root(self):
        return os.path.join(self.get_install_dir(), 'resources', 'cassandra')

    def get_node_cassandra_root(self):
        return os.path.join(self.get_path(), 'resources', 'cassandra')

    def get_conf_dir(self):
        """
        Returns the path to the directory where Cassandra config are located
        """
        return os.path.join(self.get_path(), 'resources', 'cassandra', 'conf')

    def get_tool(self, toolname):
        return common.join_bin(os.path.join(self.get_install_dir(), 'resources', 'cassandra'), 'bin', toolname)

    def get_tool_args(self, toolname):
        return [common.join_bin(os.path.join(self.get_install_dir(), 'resources', 'cassandra'), 'bin', toolname)]

    def get_env(self):
        env = self.make_hcd_env(self.get_install_dir(), self.get_path())
        # adjust JAVA_HOME to one supported by this hcd_version
        env = common.update_java_version(jvm_version=None,
                                         install_dir=self.get_install_dir(),
                                         env=env,
                                         info_message=self.name)
        return env

    def node_setup(self, version, verbose):
        dir, v = setup_hcd(version, verbose=verbose)
        return dir

    def get_launch_bin(self):
        cdir = self.get_install_dir()
        launch_bin = common.join_bin(cdir, 'bin', 'hcd')
        shutil.copy(launch_bin, self.get_bin_dir())
        return common.join_bin(self.get_path(), 'bin', 'hcd')

    def add_custom_launch_arguments(self, args):
        args.append('cassandra')

    def copy_config_files(self):
        for product in ['hcd', 'cassandra']:
            src_conf = os.path.join(self.get_install_dir(), 'resources', product, 'conf')
            dst_conf = os.path.join(self.get_path(), 'resources', product, 'conf')
            if not os.path.isdir(src_conf):
                continue
            if os.path.isdir(dst_conf):
                common.rmdirs(dst_conf)
            shutil.copytree(src_conf, dst_conf)

    def import_bin_files(self):
        common.copy_directory(os.path.join(self.get_install_dir(), 'bin'), self.get_bin_dir())
        cassandra_bin_dir = os.path.join(self.get_path(), 'resources', 'cassandra', 'bin')
        shutil.rmtree(cassandra_bin_dir, ignore_errors=True)
        os.makedirs(cassandra_bin_dir)
        common.copy_directory(os.path.join(self.get_install_dir(), 'resources', 'cassandra', 'bin'), cassandra_bin_dir)
        if os.path.exists(os.path.join(self.get_install_dir(), 'resources', 'cassandra', 'tools')):
            cassandra_tools_dir = os.path.join(self.get_path(), 'resources', 'cassandra', 'tools')
            shutil.rmtree(cassandra_tools_dir, ignore_errors=True)
            shutil.copytree(os.path.join(self.get_install_dir(), 'resources', 'cassandra', 'tools'), cassandra_tools_dir)
        self.export_hcd_home_in_hcd_env_sh()


    def export_hcd_home_in_hcd_env_sh(self):
        '''
        Due to the way CCM lays out files, separating the repository
        from the node(s) confs, the `hcd-env.sh` script of each node
        needs to have its HCD_HOME var set and exported.
        The stock `hcd-env.sh` file includes a commented-out
        place to do exactly this, intended for installers.
        Basically: read in the file, write it back out and add the two
        lines.
        'sstableloader' is an example of a node script that depends on
        this, when used in a CCM-built cluster.
        '''
        with open(self.get_bin_dir() + "/hcd-env.sh", "r") as hcd_env_sh:
            buf = hcd_env_sh.readlines()

        with open(self.get_bin_dir() + "/hcd-env.sh", "w") as out_file:
            for line in buf:
                out_file.write(line)
                if line == "# This is here so the installer can force set HCD_HOME\n":
                    out_file.write("HCD_HOME=" + self.get_install_dir() + "\nexport HCD_HOME\n")


    def _update_yaml(self):
        super(HcdNode, self)._update_yaml()
        conf_file = os.path.join(self.get_path(), 'resources', 'cassandra', 'conf', 'cassandra.yaml')
        with open(conf_file, 'r') as f:
            data = yaml.safe_load(f)
        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)

    def _get_directories(self):
        dirs = []
        for i in ['commitlogs', 'saved_caches', 'logs', 'bin', 'resources']:
            dirs.append(os.path.join(self.get_path(), i))
        for x in xrange(0, self.cluster.data_dir_count):
            dirs.append(os.path.join(self.get_path(), 'data{0}'.format(x)))
        return dirs

    def make_hcd_env(self, install_dir, node_path):
        env = os.environ.copy()
        env['MAX_HEAP_SIZE'] = os.environ.get('CCM_MAX_HEAP_SIZE', '500M')
        env['HEAP_NEWSIZE'] = os.environ.get('CCM_HEAP_NEWSIZE', '50M')
        env['MAX_DIRECT_MEMORY'] = os.environ.get('CCM_MAX_DIRECT_SIZE', '2048M')
        env['HCD_HOME'] = os.path.join(install_dir)
        env['HCD_CONF'] = os.path.join(node_path, 'resources', 'hcd', 'conf')
        env['CASSANDRA_HOME'] = os.path.join(install_dir, 'resources', 'cassandra')
        env['CASSANDRA_CONF'] = os.path.join(node_path, 'resources', 'cassandra', 'conf')
        env['HCD_LOG_ROOT'] = os.path.join(node_path, 'logs', 'hcd')
        env['CASSANDRA_LOG_DIR'] = os.path.join(node_path, 'logs')
        return env

def download_hcd_version( version, verbose=False):
    url = HCD_ARCHIVE
    if repository.CCM_CONFIG.has_option('repositories', 'hcd'):
        url = repository.CCM_CONFIG.get('repositories', 'hcd')

    url = url % version
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        repository.__download(url, target, show_progress=verbose)
        common.debug("Extracting {} as version {} ...".format(target, version))
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]  # pylint: disable=all
        tar.extractall(path=repository.__get_dir())
        tar.close()
        target_dir = os.path.join(repository.__get_dir(), version)
        if os.path.exists(target_dir):
            rmdirs(target_dir)
        shutil.move(os.path.join(repository.__get_dir(), dir), target_dir)
    except urllib.error.URLError as e:
        msg = "Invalid version %s" % version if url is None else "Invalid url %s" % url
        msg = msg + " (underlying error is: %s)" % str(e)
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError("Unable to uncompress downloaded file: %s" % str(e))

def setup_hcd(version, verbose=False):
    cdir = repository.version_directory(version)
    if cdir is None:
        download_hcd_version(version, verbose=verbose)
        cdir = repository.version_directory(version)
    return (cdir, version)


def get_hcd_version(install_dir):
    for root, dirs, files in os.walk(install_dir):
        for file in files:
            match = re.search('^hcd(?:-core)?-([0-9.]+)(?:-.*)?\.jar', file)
            if match:
                return match.group(1)
    return None


def get_hcd_cassandra_version(install_dir):
    hcd_cmd = os.path.join(install_dir, 'bin', 'hcd')
    (output, stderr) = subprocess.Popen([hcd_cmd, "cassandra", '-v'], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE).communicate()
    output = output.rstrip()
    match = re.search('([0-9.]+)(?:-.*)?', str(output))
    if match:
        return LooseVersion(match.group(1))

    raise ArgumentError("Unable to determine Cassandra version in: %s.\n\tstdout: '%s'\n\tstderr: '%s'"
                        % (install_dir, output, stderr))
