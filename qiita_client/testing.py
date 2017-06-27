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


class PluginTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client_id = '19ndkO3oMKsoChjVVWluF7QkxHRfYhTKSFbAVt8IhK7gZgDaO4'
        cls.client_secret = ('J7FfQ7CQdOxuKhQAf1eoGgBAE81Ns8Gu3EKaWFm3IO2JKh'
                             'AmmCWZuabe0O5Mp28s1')
        cls.server_cert = environ.get('QIITA_SERVER_CERT', None)
        cls.qclient = QiitaClient("https://localhost:21174", cls.client_id,
                                  cls.client_secret,
                                  server_cert=cls.server_cert)
        cls.qclient.post('/apitest/reload_plugins/')
        # Give enough time for the plugins to register
        sleep(3)

    @classmethod
    def tearDownClass(cls):
        # Reset the test database
        cls.qclient.post("/apitest/reset/")

    def _wait_for_running_job(self, job_id):
        """Waits until the given job is not in a running status

        Parameters
        ----------
        job_id : str
            The job it to wait for

        Returns
        -------
        str
            The status of the job

        Notes
        -----
        This function only polls for five seconds. After those five seconds,
        it returns whatever the last seen status for the given job
        """
        for i in range(10):
            sleep(0.5)
            status = self.qclient.get_job_info(job_id)['status']
            if status != 'running':
                break

        return status
