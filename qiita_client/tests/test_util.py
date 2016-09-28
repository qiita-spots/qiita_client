# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import TestCase, main
from os import getcwd, close
from tempfile import mkstemp

from qp_target_gene.util import system_call, get_sample_names_by_run_prefix


class UtilTests(TestCase):
    def test_system_call(self):
        obs_out, obs_err, obs_val = system_call("pwd")
        self.assertEqual(obs_out, "%s\n" % getcwd())
        self.assertEqual(obs_err, "")
        self.assertEqual(obs_val, 0)

    def test_system_call_error(self):
        obs_out, obs_err, obs_val = system_call("IHopeThisCommandDoesNotExist")
        self.assertEqual(obs_out, "")
        self.assertTrue("not found" in obs_err)
        self.assertEqual(obs_val, 127)

    def test_get_sample_names_by_run_prefix(self):
        fd, fp = mkstemp()
        close(fd)
        with open(fp, 'w') as f:
            f.write(MAPPING_FILE)
        self._clean_up_files.append(fp)

        obs = get_sample_names_by_run_prefix(fp)
        exp = {'s3': 'SKB7.640196', 's2': 'SKD8.640184', 's1': 'SKB8.640193'}
        self.assertEqual(obs, exp)

    def test_get_sample_names_by_run_prefix_error(self):
        fd, fp = mkstemp()
        close(fd)
        with open(fp, 'w') as f:
            f.write(MAPPING_FILE_2)
        self._clean_up_files.append(fp)

        with self.assertRaises(ValueError):
            get_sample_names_by_run_prefix(fp)


MAPPING_FILE = (
    "#SampleID\tplatform\tbarcode\texperiment_design_description\t"
    "library_construction_protocol\tcenter_name\tprimer\trun_prefix\t"
    "instrument_model\tDescription\n"
    "SKB7.640196\tILLUMINA\tA\tA\tA\tANL\tA\ts3\tIllumina MiSeq\tdesc1\n"
    "SKB8.640193\tILLUMINA\tA\tA\tA\tANL\tA\ts1\tIllumina MiSeq\tdesc2\n"
    "SKD8.640184\tILLUMINA\tA\tA\tA\tANL\tA\ts2\tIllumina MiSeq\tdesc3\n"
)

MAPPING_FILE_2 = (
    "#SampleID\tplatform\tbarcode\texperiment_design_description\t"
    "library_construction_protocol\tcenter_name\tprimer\t"
    "run_prefix\tinstrument_model\tDescription\n"
    "SKB7.640196\tILLUMINA\tA\tA\tA\tANL\tA\ts3\tIllumina MiSeq\tdesc1\n"
    "SKB8.640193\tILLUMINA\tA\tA\tA\tANL\tA\ts1\tIllumina MiSeq\tdesc2\n"
    "SKD8.640184\tILLUMINA\tA\tA\tA\tANL\tA\ts1\tIllumina MiSeq\tdesc3\n"
)

if __name__ == '__main__':
    main()
