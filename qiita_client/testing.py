# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import TestCase
from os import environ, sep
from os.path import join, isabs
from time import sleep

from qiita_client import QiitaClient
from .plugin import BaseQiitaPlugin

import logging

logger = logging.getLogger(__name__)
URL = "https://localhost:8383"


class PluginTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        logger.debug('Entered PluginTestCase.setUpClass()')
        cls.client_id = '19ndkO3oMKsoChjVVWluF7QkxHRfYhTKSFbAVt8IhK7gZgDaO4'
        cls.client_secret = ('J7FfQ7CQdOxuKhQAf1eoGgBAE81Ns8Gu3EKaWFm3IO2JKh'
                             'AmmCWZuabe0O5Mp28s1')
        # qiita_port needs to be the main worker in qiita, the default is 21174
        qiita_port = environ.get('QIITA_PORT', '21174')
        cls.ca_cert = environ.get('QIITA_ROOTCA_CERT')

        url = URL.replace('8383', qiita_port)
        cls.qclient = QiitaClient(
            url, cls.client_id, cls.client_secret, cls.ca_cert)

        logger.debug(
            'PluginTestCase.setUpClass() token %s' % cls.qclient._token)
        cls.qclient.post('/apitest/reload_plugins/')

        # When testing, we access plugin functions often directly. Plugin
        # configuration files are not parsed in these cases. To be able to
        # change the plugin coupling protocol, we resort to the environment
        # variable here.
        cls.qclient._plugincoupling = environ.get(
            'QIITA_PLUGINCOUPLING', BaseQiitaPlugin._DEFAULT_PLUGIN_COUPLINGS)

        # Give enough time for the plugins to register
        sleep(5)

    @classmethod
    def tearDownClass(cls):
        logger.debug('Entered PluginTestCase.tearDownClass()')
        # Reset the test database
        cls.qclient.post("/apitest/reset/")

    def _wait_for_running_job(self, job_id):
        """Waits until the given job is not in a running status

        Parameters
        ----------
        job_id : str
            The job id to wait for

        Returns
        -------
        str
            The status of the job

        Notes
        -----
        This function only polls for five seconds. After those five seconds,
        it returns whatever was the last seen status for the given job
        """
        logger.debug('Entered PluginTestCase._wait_for_running_job()')
        for i in range(20):
            logger.debug('Try get_job_info %d' % i)
            sleep(0.5)
            status = self.qclient.get_job_info(job_id)['status']
            if status != 'running':
                logger.debug('Job status: %s' % status)
                break

        return status

    def deposite_in_qiita_basedir(self, fps, update_fp_only=False):
        """Pushs a file to qiita main AND adapts given filepath accordingly.

        A helper function to fix file paths in tests such that they point to
        the expected BASE_DATA_DIR. This becomes necessary when uncoupling the
        plugin filesystem as some methods now actually fetches expected files
        from BASE_DATA_DIR. This will fail for protocols other than filesystem
        IF files are created locally by the plugin test.

        Parameters
        ----------
        fps : str or [str]
            Filepath or list of filepaths to file(s) that shall be part of
            BASE_DATA_DIR, but currently points to some tmp file for testing.
        update_fp_only : bool
            Some tests operate on filepaths only - files do not actually need
            to exist. Thus, we don't need to tranfer a file.

        Returns
        -------
        The potentially modified filepaths.
        """
        def _stripRoot(fp):
            # chop off leading / for join to work properly when prepending
            # the BASE_DATA_DIR
            if isabs(fp):
                return fp[len(sep):]
            return fp

        if self.qclient._plugincoupling == 'filesystem':
            return fps

        # use artifact 1 info to determine BASA_DATA_DIR, as we know that the
        # filepath ends with ....raw_data/1_s_G1_L001_sequences.fastq.gz, thus
        # BASE_DATA_DIR must be the prefix, e.g. /qiita_data/
        # This might break IF file
        #    qiita-spots/qiita/qiita_db/support_files/populate_test_db.sql
        # changes.
        ainfo = self.qclient.get('/qiita_db/artifacts/1/')
        base_data_dir = ainfo['files']['raw_forward_seqs'][0]['filepath'][
            :(-1 * len('raw_data/1_s_G1_L001_sequences.fastq.gz'))]
        if isinstance(fps, str):
            if not update_fp_only:
                self.qclient.push_file_to_central(fps)
            return join(base_data_dir, _stripRoot(fps))
        elif isinstance(fps, list):
            for fp in fps:
                if not update_fp_only:
                    self.qclient.push_file_to_central(fp)
            return [join(base_data_dir, _stripRoot(fp)) for fp in fps]
        else:
            raise ValueError(
                "deposite_in_qiita_basedir is not implemented for type %s"
                % type(fps))
