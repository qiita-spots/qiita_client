# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import TestCase, main

from qiita_client import QiitaPlugin, QiitaTypePlugin, QiitaCommand


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


class QiitaPluginTest(TestCase):
    pass


class QiitaTypePluginTest(TestCase):
    pass

if __name__ == '__main__':
    main()
