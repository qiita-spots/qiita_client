# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import TestCase, main

from os.path import isdir, join, exists, basename
from os import remove
from shutil import rmtree

from qiita_client.testing import PluginTestCase
from qiita_client import (QiitaPlugin, QiitaTypePlugin, QiitaCommand,
                          QiitaArtifactType)


class QiitaCommandTest(TestCase):
    def setUp(self):
        self.exp_req = {'p1': ('artifact', ['FASTQ'])}
        self.exp_opt = {'p2': ('boolean', 'False'),
                        'p3': ('string', 'somestring')}
        self.exp_dflt = {'dflt1': {'p2': 'True', 'p3': 'anotherstring'}}

    def test_init(self):
        # Create a test function
        def func(a, b, c, d):
            return 42

        obs = QiitaCommand("Test cmd", "Some description", func, self.exp_req,
                           self.exp_opt, self.exp_dflt)
        self.assertEqual(obs.name, "Test cmd")
        self.assertEqual(obs.description, "Some description")
        self.assertEqual(obs.function, func)
        self.assertEqual(obs.required_parameters, self.exp_req)
        self.assertEqual(obs.optional_parameters, self.exp_opt)
        self.assertEqual(obs.default_parameter_sets, self.exp_dflt)

        with self.assertRaises(TypeError):
            QiitaCommand("Name", "Desc", "func", self.exp_req, self.exp_opt,
                         self.exp_dflt)

        def func(a, b, c):
            return 42

        with self.assertRaises(ValueError):
            QiitaCommand("Name", "Desc", func, self.exp_req, self.exp_opt,
                         self.exp_dflt)

    def test_call(self):
        def func(a, b, c, d):
            return 42

        obs = QiitaCommand("Test cmd", "Some description", func, self.exp_req,
                           self.exp_opt, self.exp_dflt)
        self.assertEqual(obs('a', 'b', 'c', 'd'), 42)


class QiitaArtifactTypeTest(TestCase):
    def test_init(self):
        obs = QiitaArtifactType('Name', 'Description', False, True,
                                [('plain_text', False)])
        self.assertEqual(obs.name, 'Name')
        self.assertEqual(obs.description, 'Description')
        self.assertFalse(obs.ebi)
        self.assertTrue(obs.vamps)
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

        atypes = [QiitaArtifactType('Name', 'Description', False, True,
                                    [('plain_text', False)])]
        obs = QiitaTypePlugin("NewPlugin", "1.0.0", "Description",
                              validate_func, html_generator_func,
                              atypes)
        self.assertEqual(obs.name, "NewPlugin")
        self.assertEqual(obs.version, "1.0.0")
        self.assertEqual(obs.description, "Description")
        self.assertItemsEqual(obs.task_dict.keys(),
                              ['Validate', 'Generate HTML summary'])
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
        atypes = [QiitaArtifactType('Name', 'Description', False, True,
                                    [('plain_text', False)])]
        tester = QiitaTypePlugin("NewPlugin", "1.0.0", "Description",
                                 validate_func, html_generator_func, atypes)

        tester.generate_config('env_script', 'start_script')
        self.assertTrue(exists(tester.conf_fp))
        with open(tester.conf_fp, 'U') as f:
            conf = f.readlines()

        exp_lines = ['[main]\n',
                     'NAME = NewPlugin\n',
                     'VERSION = 1.0.0\n',
                     'DESCRIPTION = Description\n',
                     'ENVIRONMENT_SCRIPT = env_script\n',
                     'START_SCRIPT = start_script\n',
                     'PLUGIN_TYPE = artifact definition\n',
                     'PUBLICATIONS = \n',
                     '\n',
                     '[oauth2]\n',
                     'SERVER_CERT = \n']
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
        atypes = [QiitaArtifactType('Name', 'Description', False, True,
                                    [('plain_text', False)])]
        tester = QiitaTypePlugin("NewPlugin", "1.0.0", "Description",
                                 validate_func, html_generator_func, atypes)

        # Generate the config file for the new plugin
        tester.generate_config('env_script', 'start_script',
                               server_cert=self.server_cert)
        # Ask Qiita to reload the plugins
        self.qclient.post('/apitest/reload_plugins/')

        # Install the current plugin
        tester("https://localhost:21174", 'register', 'ignored')

        # Check that it has been installed
        obs = self.qclient.get('/qiita_db/plugins/NewPlugin/1.0.0/')
        self.assertEqual(obs['name'], 'NewPlugin')


class QiitaPluginTest(PluginTestCase):
    pass

if __name__ == '__main__':
    main()
