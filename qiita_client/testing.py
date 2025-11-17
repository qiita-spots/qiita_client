# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import TestCase
from os import environ
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

        # Determine BASE_DATA_DIR of qiita central, without having direct
        # access to qiita's settings file. This is done by requesting
        # information about prep 1, which should be in the test database.
        # This might break IF file
        #    qiita-spots/qiita/qiita_db/support_files/populate_test_db.sql
        # changes.
        prep_info = cls.qclient.get('/qiita_db/prep_template/1/',
                                    no_file_fetching=True)
        cls.base_data_dir = prep_info['prep-file'].split('templates/')[0]

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
