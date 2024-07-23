# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import TestCase, main
from os.path import isdir, exists, basename, join
from os import remove
from shutil import rmtree
from json import dumps
from tempfile import mkdtemp

from qiita_client.testing import PluginTestCase
from qiita_client import (QiitaPlugin, QiitaTypePlugin, QiitaCommand,
                          QiitaArtifactType, ArtifactInfo)


class QiitaCommandTest(TestCase):
    def setUp(self):
        self.exp_req = {'p1': ('artifact', ['FASTQ'])}
        self.exp_opt = {'p2': ('boolean', 'False'),
                        'p3': ('string', 'somestring')}
        self.exp_dflt = {'dflt1': {'p2': 'True', 'p3': 'anotherstring'}}
        self.exp_out = {'out1': 'BIOM'}

    def test_init(self):
        # Create a test function
        def func(a, b, c, d):
            return 42

        obs = QiitaCommand("Test cmd", "Some description", func, self.exp_req,
                           self.exp_opt, self.exp_out, self.exp_dflt)
        self.assertEqual(obs.name, "Test cmd")
        self.assertEqual(obs.description, "Some description")
        self.assertEqual(obs.function, func)
        self.assertEqual(obs.required_parameters, self.exp_req)
        self.assertEqual(obs.optional_parameters, self.exp_opt)
        self.assertEqual(obs.outputs, self.exp_out)
        self.assertEqual(obs.default_parameter_sets, self.exp_dflt)
        self.assertFalse(obs.analysis_only)

        obs = QiitaCommand("Test cmd analysis", "Some description", func,
                           self.exp_req, self.exp_opt, self.exp_out,
                           self.exp_dflt, analysis_only=True)
        self.assertEqual(obs.name, "Test cmd analysis")
        self.assertEqual(obs.description, "Some description")
        self.assertEqual(obs.function, func)
        self.assertEqual(obs.required_parameters, self.exp_req)
        self.assertEqual(obs.optional_parameters, self.exp_opt)
        self.assertEqual(obs.outputs, self.exp_out)
        self.assertEqual(obs.default_parameter_sets, self.exp_dflt)
        self.assertTrue(obs.analysis_only)

        with self.assertRaises(TypeError):
            QiitaCommand("Name", "Desc", "func", self.exp_req, self.exp_opt,
                         self.exp_out, self.exp_dflt)

        def func(a, b, c):
            return 42

        with self.assertRaises(ValueError):
            QiitaCommand("Name", "Desc", func, self.exp_req, self.exp_opt,
                         self.exp_out, self.exp_dflt)

    def test_call(self):
        def func(a, b, c, d):
            return 42

        obs = QiitaCommand("Test cmd", "Some description", func, self.exp_req,
                           self.exp_opt, self.exp_out, self.exp_dflt)
        self.assertEqual(obs('a', 'b', 'c', 'd'), 42)


class QiitaArtifactTypeTest(TestCase):
    def test_init(self):
        obs = QiitaArtifactType('Name', 'Description', False, True, True,
                                [('plain_text', False)])
        self.assertEqual(obs.name, 'Name')
        self.assertEqual(obs.description, 'Description')
        self.assertFalse(obs.ebi)
        self.assertTrue(obs.vamps)
        self.assertTrue(obs.is_user_uploadable)
        self.assertEqual(obs.fp_types, [('plain_text', False)])


class QiitaTypePluginTest(PluginTestCase):
    def setUp(self):
        self.clean_up_fp = []

    def tearDown(self):
        for fp in self.clean_up_fp:
            if exists(fp):
                if isdir(fp):
                    rmtree(fp)
                else:
                    remove(fp)

    def test_init(self):
        def validate_func(a, b, c, d):
            return 42

        def html_generator_func(a, b, c, d):
            return 42

        atypes = [QiitaArtifactType('Name', 'Description', False, True, False,
                                    [('plain_text', False)])]
        obs = QiitaTypePlugin("NewPlugin", "1.0.0", "Description",
                              validate_func, html_generator_func,
                              atypes)
        self.assertEqual(obs.name, "NewPlugin")
        self.assertEqual(obs.version, "1.0.0")
        self.assertEqual(obs.description, "Description")
        self.assertEqual(set(obs.task_dict.keys()),
                         {'Validate', 'Generate HTML summary'})
        self.assertEqual(obs.task_dict['Validate'].function, validate_func)
        self.assertEqual(obs.task_dict['Generate HTML summary'].function,
                         html_generator_func)
        self.assertEqual(obs.artifact_types, atypes)
        self.assertEqual(basename(obs.conf_fp), 'NewPlugin_1.0.0.conf')

    def test_generate_config(self):
        def validate_func(a, b, c, d):
            return 42

        def html_generator_func(a, b, c, d):
            return 42
        atypes = [QiitaArtifactType('Name', 'Description', False, True, False,
                                    [('plain_text', False)])]
        tester = QiitaTypePlugin("NewPlugin", "1.0.0", "Description",
                                 validate_func, html_generator_func, atypes)

        tester.generate_config('ls', 'echo')
        self.assertTrue(exists(tester.conf_fp))
        with open(tester.conf_fp, 'U') as f:
            conf = f.readlines()

        exp_lines = ['[main]\n',
                     'NAME = NewPlugin\n',
                     'VERSION = 1.0.0\n',
                     'DESCRIPTION = Description\n',
                     'ENVIRONMENT_SCRIPT = ls\n',
                     'START_SCRIPT = echo\n',
                     'PLUGIN_TYPE = artifact definition\n',
                     'PUBLICATIONS = \n',
                     '\n',
                     '[oauth2]\n']
        # We will test the last 2 lines independently since they're variable
        # in each test run
        self.assertEqual(conf[:-2], exp_lines)
        self.assertTrue(conf[-2].startswith('CLIENT_ID = '))
        self.assertTrue(conf[-1].startswith('CLIENT_SECRET = '))

    def test_call(self):
        def validate_func(a, b, c, d):
            return 42

        def html_generator_func(a, b, c, d):
            return 42

        # Test the install procedure
        atypes = [QiitaArtifactType('Name', 'Description', False, True, False,
                                    [('plain_text', False)])]
        tester = QiitaTypePlugin("NewPlugin", "1.0.0", "Description",
                                 validate_func, html_generator_func, atypes)

        # Generate the config file for the new plugin
        tester.generate_config('ls', 'echo')
        # Ask Qiita to reload the plugins
        self.qclient.post('/apitest/reload_plugins/')

        # Install the current plugin
        tester("https://localhost:21174", 'register', 'ignored')

        # Check that it has been installed
        obs = self.qclient.get('/qiita_db/plugins/NewPlugin/1.0.0/')
        self.assertEqual(obs['name'], 'NewPlugin')
        self.assertEqual(obs['version'], '1.0.0')


class QiitaPluginTest(PluginTestCase):
    # Most of the functionality is being tested in the previous
    # class. Here we are going to test that we can actually execute a job
    def setUp(self):
        self.outdir = mkdtemp()

    def tearDown(self):
        rmtree(self.outdir)

    def test_call(self):
        def func(qclient, job_id, job_params, working_dir):
            fp = join(working_dir, 'test.fastq')
            with open(fp, 'w') as f:
                f.write('')
            res = ArtifactInfo('out1', 'Demultiplexed',
                               [[fp, 'preprocessed_fastq']])
            return True, "", [res]

        tester = QiitaPlugin("NewPlugin", "0.0.1", "description")
        cmd = QiitaCommand("NewCmd", "Desc", func,
                           {'p1': ('artifact', ['FASTQ'])},
                           {'p2': ('string', 'dflt')},
                           {'out1': 'Demultiplexed'})
        tester.register_command(cmd)

        a_cmd = QiitaCommand("NewCmdAnalysis", "Desc", func,
                             {'p1': ('artifact', ['FASTQ'])},
                             {'p2': ('string', 'dflt')},
                             {'out1': 'Demultiplexed'})
        tester.register_command(a_cmd)

        tester.generate_config('ls', 'echo')
        self.qclient.post('/apitest/reload_plugins/')
        tester("https://localhost:21174", 'register', 'ignored')

        obs = self.qclient.get('/qiita_db/plugins/NewPlugin/0.0.1/')
        self.assertEqual(obs['name'], 'NewPlugin')
        self.assertEqual(obs['version'], '0.0.1')
        # I can't use assertItemsEqual because it is not available in py3
        # and I can't user assertCountEqual because it is not avaialable in py2
        self.assertEqual(sorted(obs['commands']),
                         sorted(['NewCmd', 'NewCmdAnalysis']))

        # Create a new job
        data = {'command': dumps(['NewPlugin', '0.0.1', 'NewCmd']),
                'parameters': dumps({'p1': '1', 'p2': 'a'}),
                'status': 'queued'}
        job_id = self.qclient.post('/apitest/processing_job/',
                                   data=data)['job']
        tester("https://localhost:21174", job_id, self.outdir)

        status = self._wait_for_running_job(job_id)
        self.assertEqual(status, 'success')


if __name__ == '__main__':
    main()
