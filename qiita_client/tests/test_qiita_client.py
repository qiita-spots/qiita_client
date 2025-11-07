# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import TestCase, main
import filecmp
from os import remove, close, makedirs
from os.path import basename, exists, expanduser, join, isdir, dirname
from tempfile import mkstemp
from json import dumps
import pandas as pd
from shutil import rmtree
from pathlib import Path

from qiita_client.qiita_client import (QiitaClient, _format_payload,
                                       ArtifactInfo)
from qiita_client.testing import PluginTestCase, URL
from qiita_client.exceptions import BadRequestError

CLIENT_ID = '19ndkO3oMKsoChjVVWluF7QkxHRfYhTKSFbAVt8IhK7gZgDaO4'
BAD_CLIENT_ID = 'NOT_A_CLIENT_ID'
CLIENT_SECRET = ('J7FfQ7CQdOxuKhQAf1eoGgBAE81Ns8Gu3EKaWFm3IO2JKh'
                 'AmmCWZuabe0O5Mp28s1')


class ArtifactInfoTests(TestCase):
    def test_init(self):
        files = [("fp1", "preprocessed_fasta"), ("fp2", "preprocessed_fastq")]
        obs = ArtifactInfo('demultiplexed', 'Demultiplexed', files)
        self.assertEqual(obs.output_name, 'demultiplexed')
        self.assertEqual(obs.artifact_type, 'Demultiplexed')
        self.assertEqual(obs.files, files)

    def test_eq_ne(self):
        files = [("fp1", "preprocessed_fasta"), ("fp2", "preprocessed_fastq")]
        obs = ArtifactInfo('demultiplexed', 'Demultiplexed', files)

        self.assertEqual(obs, obs)
        self.assertEqual(
            obs, ArtifactInfo('demultiplexed', 'Demultiplexed', files))
        files2 = [("fp2", "preprocessed_fastq"), ("fp1", "preprocessed_fasta")]
        obs2 = ArtifactInfo('demultiplexed', 'Demultiplexed', files2)
        self.assertEqual(obs, obs2)

        self.assertNotEqual(obs, 1)
        self.assertNotEqual(obs, ArtifactInfo('demux', 'Demultiplexed', files))
        self.assertNotEqual(obs, ArtifactInfo('demultiplexed', 'Demux', files))

        obs3 = ArtifactInfo(
            'demultiplexed', 'Demultiplexed', files, {'AA': ['aa'], 'CA': 'c'})
        obs4 = ArtifactInfo(
            'demultiplexed', 'Demultiplexed', files, {'AA': ['aa'], 'CA': 'c'})
        obs5 = ArtifactInfo(
            'demultiplexed', 'Demultiplexed', files, {'AA': ['aa'], 'CA': 'C'})
        self.assertNotEqual(obs3, obs)
        self.assertEqual(obs3, obs4)
        self.assertNotEqual(obs3, obs5)
        self.assertNotEqual(obs4, obs5)


class UtilTests(TestCase):
    def test_format_payload(self):
        ainfo = [ArtifactInfo("demultiplexed", "Demultiplexed",
                              [("fp1", "preprocessed_fasta"),
                               ("fp2", "preprocessed_fastq")])]
        obs = _format_payload(True, artifacts_info=ainfo, error_msg="Ignored")
        exp = {'success': True, 'error': '',
               'artifacts':
                   {'demultiplexed':
                       {'artifact_type': "Demultiplexed",
                        'filepaths': [("fp1", "preprocessed_fasta"),
                                      ("fp2", "preprocessed_fastq")],
                        'archive': {}}}}
        self.assertEqual(obs, exp)

        ainfo = [ArtifactInfo("demultiplexed", "Demultiplexed",
                              [("fp1", "preprocessed_fasta"),
                               ("fp2", "preprocessed_fastq")],
                              {'AA': ['aa'], 'CA': 'ca'})]
        obs = _format_payload(True, artifacts_info=ainfo, error_msg="Ignored")
        exp = {'success': True, 'error': '',
               'artifacts':
                   {'demultiplexed':
                       {'artifact_type': "Demultiplexed",
                        'filepaths': [("fp1", "preprocessed_fasta"),
                                      ("fp2", "preprocessed_fastq")],
                        'archive': {'AA': ['aa'], 'CA': 'ca'}}}}
        self.assertEqual(obs, exp)

    def test_format_payload_error(self):
        obs = _format_payload(False, error_msg="Some error",
                              artifacts_info=['ignored'])
        exp = {'success': False, 'error': 'Some error', 'artifacts': None}
        self.assertEqual(obs, exp)


class QiitaClientTests(PluginTestCase):
    def setUp(self):
        self.tester = QiitaClient(URL,
                                  CLIENT_ID,
                                  CLIENT_SECRET, self.ca_cert)
        self.clean_up_files = []

        # making assertRaisesRegex compatible with Python 2.7 and 3.9
        if not hasattr(self, 'assertRaisesRegex'):
            setattr(self, 'assertRaisesRegex', self.assertRaisesRegexp)

    def tearDown(self):
        for fp in self.clean_up_files:
            if exists(fp):
                if isdir(fp):
                    rmtree(fp)
                else:
                    remove(fp)

    def test_init(self):
        obs = QiitaClient(URL,
                          CLIENT_ID,
                          CLIENT_SECRET,
                          ca_cert=self.ca_cert)
        self.assertEqual(obs._server_url, URL)
        self.assertEqual(obs._client_id, CLIENT_ID)
        self.assertEqual(obs._client_secret, CLIENT_SECRET)
        self.assertEqual(obs._verify, self.ca_cert)

    def test_get(self):
        obs = self.tester.get("/qiita_db/artifacts/1/")
        exp = {
            'prep_information': [1],
            'is_submitted_to_vamps': None,
            'data_type': '18S',
            'can_be_submitted_to_vamps': False,
            'can_be_submitted_to_ebi': False,
            'timestamp': '2012-10-01 09:30:27',
            'study': 1,
            'processing_parameters': None,
            'visibility': 'private',
            'ebi_run_accessions': None,
            'type': 'FASTQ',
            'name': 'Raw data 1',
            'analysis': None}

        # Files contain the full path, which it is hard to test, so get only
        # the basename of the files
        obs_files = obs.pop('files')
        obs_files = {
            k: [{'filepath': basename(vv['filepath']),
                 'size': vv['size']} for vv in v]
            for k, v in obs_files.items()}
        exp_files = {
            'raw_barcodes': [
                {'filepath': '1_s_G1_L001_sequences_barcodes.fastq.gz',
                 'size': 58}],
            'raw_forward_seqs': [
                {'filepath': '1_s_G1_L001_sequences.fastq.gz',
                 'size': 58}]}

        self.assertEqual(obs, exp)
        self.assertEqual(obs_files, exp_files)

    def test_get_error(self):
        with self.assertRaises(RuntimeError):
            self.tester.get("/qiita_db/artifacts/1/type/")

    def test_post(self):
        obs = self.tester.post(
            "/qiita_db/jobs/bcc7ebcd-39c1-43e4-af2d-822e3589f14d/heartbeat/",
            data="")
        self.assertIsNone(obs)

    def test_post_error(self):
        with self.assertRaises(RuntimeError):
            self.tester.post("/qiita_db/artifacts/1/type/")

    def test_patch(self):
        artifact_id = '2'

        # before we patch the artifact by adding the html_summary, the
        # html_summary should be None
        self.assertIsNone(self.tester.get_artifact_html_summary(artifact_id))

        # now, let's patch
        fd, fp = mkstemp()
        close(fd)
        with open(fp, 'w') as f:
            f.write('\n')
        self.clean_up_files.append(fp)
        obs = self.tester.patch('/qiita_db/artifacts/%s/' % artifact_id, 'add',
                                '/html_summary/', value=fp)
        self.assertIsNone(obs)

        # now, the html_summary should contain the filename fp
        self.assertTrue(
            basename(fp) in self.tester.get_artifact_html_summary(artifact_id))

    def test_patch_error(self):
        with self.assertRaises(BadRequestError):
            self.tester.patch(
                '/qiita_db/artifacts/1/filepaths/', 'test',
                '/html_summary/', value='/path/to/html_summary')

    def test_patch_value_error(self):
        # Add, replace or test
        with self.assertRaises(ValueError):
            self.tester.patch(
                '/qiita_db/artifacts/1/', 'add', '/html_summary/',
                from_p='/fastq/')

        # move or copy
        with self.assertRaises(ValueError):
            self.tester.patch(
                '/qiita_db/artifacts/1/', 'move',
                '/html_summary/', value='/path/to/html_summary')

        with self.assertRaises(TypeError):
            self.tester.patch("/qiita_db/artifacts/1/type/")

    def test_http_patch(self):
        data = self.tester.get('/api/v1/study/1/samples/info')
        cats = data['categories']
        payload = dumps({'foo': {c: 'bar' for c in cats}})
        obs = self.tester.http_patch('/api/v1/study/1/samples', data=payload)
        self.assertIsNone(obs)

    def test_start_heartbeat(self):
        job_id = "063e553b-327c-4818-ab4a-adfe58e49860"
        self.tester.start_heartbeat(job_id)

    def test_get_job_info(self):
        job_id = "3c9991ab-6c14-4368-a48c-841e8837a79c"
        obs = self.tester.get_job_info(job_id)
        exp = {'command': 'Pick closed-reference OTUs',
               'msg': '',
               'status': 'success',
               'parameters': {'input_data': 2,
                              'reference': 1,
                              'similarity': 0.97,
                              'sortmerna_coverage': 0.97,
                              'sortmerna_e_value': 1,
                              'sortmerna_max_pos': 10000,
                              'threads': 1}}
        self.assertEqual(obs, exp)

    def test_update_job_step(self):
        job_id = "bcc7ebcd-39c1-43e4-af2d-822e3589f14d"
        new_step = "some new step"
        obs = self.tester.update_job_step(job_id, new_step)
        self.assertIsNone(obs)

    def test_update_job_step_ignore_failure(self):
        job_id = "bcc7ebcd-39c1-43e4-af2d-822e3589f14d"
        new_step = "some new step"

        # confirm that update_job_step behaves as before when ignore_error
        # parameter absent or set to False.
        self.bad_tester = QiitaClient(URL,
                                      BAD_CLIENT_ID,
                                      CLIENT_SECRET,
                                      self.ca_cert)

        with self.assertRaises(BaseException):
            self.bad_tester.update_job_step(
                job_id, new_step, ignore_error=False)

        with self.assertRaises(BaseException):
            self.bad_tester.update_job_step(job_id,
                                            new_step,
                                            ignore_error=False)

        # confirm that when ignore_error is set to True, an Error is NOT
        # raised.
        try:
            self.bad_tester.update_job_step(job_id,
                                            new_step,
                                            ignore_error=True)
        except BaseException as e:
            self.fail("update_job_step() raised an error: %s" % str(e))

    def test_complete_job(self):
        # Create a new job
        data = {
            'user': 'demo@microbio.me',
            'command': dumps(['QIIMEq2', '1.9.1',
                              'Pick closed-reference OTUs']),
            'status': 'running',
            'parameters': dumps({"reference": 1,
                                 "sortmerna_e_value": 1,
                                 "sortmerna_max_pos": 10000,
                                 "similarity": 0.97,
                                 "sortmerna_coverage": 0.97,
                                 "threads": 1,
                                 "input_data": 1})
            }
        res = self.tester.post('/apitest/processing_job/', data=data)
        job_id = res['job']

        # Complete it
        fd, fp = mkstemp()
        close(fd)
        with open(fp, 'w') as f:
            f.write('\n')
        self.clean_up_files.append(fp)

        ainfo = [ArtifactInfo("demultiplexed", "Demultiplexed",
                              [(fp, "preprocessed_fasta")])]

        obs = self.tester.complete_job(job_id, True, artifacts_info=ainfo)
        self.assertIsNone(obs)

    def test_artifact_and_preparation_files(self):

        # check success
        fobs, prep_info = self.tester.artifact_and_preparation_files(1)
        # just leaving filenames as the folders are dynamic and a pain to test
        fobs = {k: [basename(vv) for vv in v] for k, v in fobs.items()}
        fexp = {'raw_forward_seqs': ['1_s_G1_L001_sequences.fastq.gz'],
                'raw_barcodes': ['1_s_G1_L001_sequences_barcodes.fastq.gz']}
        self.assertEqual(fobs, fexp)
        self.assertEqual(prep_info.shape, (27, 22))

        # check failure
        with self.assertRaisesRegex(RuntimeError, 'Artifact 8 is an analysis '
                                    'artifact, this method is meant to work '
                                    'with artifacts linked to a preparation.'):
            self.tester.artifact_and_preparation_files(8)

        # test _process_files_per_sample_fastq
        # both fwd/rev
        files = {
            'raw_forward_seqs': [
                {'filepath': '/X/file_3_R1.fastq.gz', 'size': 101},
                {'filepath': '/X/file_1_R1.fastq.gz', 'size': 99},
                {'filepath': '/X/file_2_R1.fastq.gz', 'size': 101}],
            'raw_reverse_seqs': [
                {'filepath': '/X/file_2_R2.fastq.gz', 'size': 101},
                {'filepath': '/X/file_1_R2.fastq.gz', 'size': 101},
                {'filepath': '/X/file_3_R2.fastq.gz', 'size': 101}]}
        prep_info = pd.DataFrame.from_dict({
            'run_prefix': {"sample.1": 'file_1',
                           "sample.2": 'file_2',
                           "sample.3": 'file_3'}}, dtype=str)
        prep_info.index.name = 'sample_name'
        fobs, piobs = self.tester._process_files_per_sample_fastq(
            files, prep_info, False)
        fexp = {
            'sample.1': ({'filepath': '/X/file_1_R1.fastq.gz', 'size': 99},
                         {'filepath': '/X/file_1_R2.fastq.gz', 'size': 101}),
            'sample.2': ({'filepath': '/X/file_2_R1.fastq.gz', 'size': 101},
                         {'filepath': '/X/file_2_R2.fastq.gz', 'size': 101}),
            'sample.3': ({'filepath': '/X/file_3_R1.fastq.gz', 'size': 101},
                         {'filepath': '/X/file_3_R2.fastq.gz', 'size': 101})}
        self.assertEqual(fobs, fexp)
        self.assertEqual(piobs.shape, (3, 1))

        fobs, piobs = self.tester._process_files_per_sample_fastq(
            files, prep_info, True)
        del fexp['sample.1']
        self.assertEqual(fobs, fexp)
        self.assertEqual(piobs.shape, (2, 1))

        # just fwd
        files = {
            'raw_forward_seqs': [
                {'filepath': '/X/file_3_R1.fastq.gz', 'size': 101},
                {'filepath': '/X/file_1_R1.fastq.gz', 'size': 99},
                {'filepath': '/X/file_2_R1.fastq.gz', 'size': 101}]}
        prep_info = pd.DataFrame.from_dict({
            'run_prefix': {"sample.1": 'file_1',
                           "sample.2": 'file_2',
                           "sample.3": 'file_3'}}, dtype=str)
        prep_info.index.name = 'sample_name'
        fobs, piobs = self.tester._process_files_per_sample_fastq(
            files, prep_info, False)
        fexp = {
            'sample.1': ({'filepath': '/X/file_1_R1.fastq.gz', 'size': 99},
                         None),
            'sample.2': ({'filepath': '/X/file_2_R1.fastq.gz', 'size': 101},
                         None),
            'sample.3': ({'filepath': '/X/file_3_R1.fastq.gz', 'size': 101},
                         None)}
        self.assertEqual(fobs, fexp)
        self.assertEqual(piobs.shape, (3, 1))

        fobs, piobs = self.tester._process_files_per_sample_fastq(
            files, prep_info, True)
        del fexp['sample.1']
        self.assertEqual(fobs, fexp)
        self.assertEqual(piobs.shape, (2, 1))

    def test_fetch_file_from_central(self):
        self.tester._plugincoupling = 'filesystem'

        ainfo = self.tester.get("/qiita_db/artifacts/%s/" % 1)
        fp = ainfo['files']['raw_forward_seqs'][0]['filepath']

        # mode: filesystem, prefix='': no copy, directly return given fp
        fp_obs = self.tester.fetch_file_from_central(fp)
        self.assertEqual(fp, fp_obs)

        # mode: filesystem, prefix='/karl': make file copy
        prefix = join(expanduser("~"), 'karl')
        self.clean_up_files.append(prefix + fp)
        fp_obs = self.tester.fetch_file_from_central(fp, prefix=prefix)
        self.assertEqual(prefix + fp, fp_obs)
        self.assertTrue(filecmp.cmp(fp, fp_obs, shallow=False))

        # non existing mode
        with self.assertRaises(ValueError):
            self.tester._plugincoupling = 'foo'
            self.tester.fetch_file_from_central(fp)

        # change transfer mode to https
        self.tester._plugincoupling = 'https'
        prefix = join(expanduser("~"), 'kurt')
        self.clean_up_files.append(prefix + fp)
        fp_obs = self.tester.fetch_file_from_central(fp, prefix=prefix)
        self.assertEqual(prefix + fp, fp_obs)
        self.assertTrue(filecmp.cmp(fp, fp_obs, shallow=False))

    def test_push_file_to_central(self):
        self.tester._plugincoupling = 'filesystem'

        ainfo = self.tester.get("/qiita_db/artifacts/%s/" % 1)
        fp = ainfo['files']['raw_forward_seqs'][0]['filepath']

        # mode: filesystem
        fp_obs = self.tester.push_file_to_central(fp)
        self.assertEqual(fp, fp_obs)

        # non existing mode
        with self.assertRaises(ValueError):
            self.tester._plugincoupling = 'foo'
            self.tester.push_file_to_central(fp)

        # change transfer mode to https
        self.tester._plugincoupling = 'https'
        fp_source = 'foo.bar'
        with open(fp_source, 'w') as f:
            f.write("this is a test\n")
        self.clean_up_files.append(fp_source)
        fp_obs = self.tester.push_file_to_central(fp_source)
        self.assertEqual(fp_source, fp_obs)

    def _create_test_dir(self, prefix=None):
        """Creates a test directory with files and subdirs."""
        # prefix
        # |- testdir/
        # |---- fileA.txt
        # |---- subdirA_l1/
        # |-------- fileB.fna
        # |-------- subdirC_l2/
        # |------------ fileC.log
        # |------------ fileD.seq
        # |---- subdirB_l1/
        # |-------- fileE.sff
        if (prefix is not None) and (prefix != ""):
            prefix = join(prefix, 'testdir')
        else:
            prefix = 'testdir'

        for dir in [join(prefix, 'subdirA_l1', 'subdirC_l2'),
                    join(prefix, 'subdirB_l1')]:
            if not exists(dir):
                makedirs(dir)
        for file, cont in [(join(prefix, 'fileA.txt'), 'contentA'),
                           (join(prefix, 'subdirA_l1',
                                 'fileB.fna'), 'this is B'),
                           (join(prefix, 'subdirA_l1', 'subdirC_l2',
                                 'fileC.log'), 'call me c'),
                           (join(prefix, 'subdirA_l1', 'subdirC_l2',
                                 'fileD.seq'), 'I d'),
                           (join(prefix, 'subdirB_l1', 'fileE.sff'), 'oh e')]:
            with open(file, "w") as f:
                f.write(cont + "\n")
        self.clean_up_files.append(prefix)

        return prefix

    def test_push_file_to_central_dir(self):
        self.tester._plugincoupling = 'https'

        fp_source = self._create_test_dir('/tmp/test_push_dir/')
        fp_obs = self.tester.push_file_to_central(fp_source)
        self.assertEqual(fp_source, fp_obs)
        # As we don't necessarily know the QIITA_BASE_DIR, we cannot fetch one
        # of the files to double check for it's content

    def test_delete_file_from_central(self):
        # obtain current filepaths to infer QIITA_BASE_DIR
        ainfo = self.tester.get("/qiita_db/artifacts/%s/" % 1)
        cwd = dirname(ainfo['files']['raw_forward_seqs'][0]['filepath'])

        for protocol in ['filesystem', 'https']:
            self.qclient._plugincoupling = protocol

            # deposit a test file
            fp_test = join(cwd, 'deleteme_%s.txt' % protocol)
            makedirs(cwd, exist_ok=True)
            with open(fp_test, 'w') as f:
                f.write('This is a testfile content\n')
            self.clean_up_files.append(fp_test)

            # sanity check that test file has been deposited correctly
            # no push required, as in this test local and remote QIITA_BASE_DIR
            # is identical
            fp_obs = self.qclient.fetch_file_from_central(fp_test)
            self.assertTrue(exists(fp_obs))

            # delete file and test if it is gone
            fp_deleted = self.qclient.delete_file_from_central(fp_test)
            if protocol == 'filesystem':
                # all three fp should point to the same filepath
                self.assertFalse(exists(fp_obs))
                self.assertFalse(exists(fp_test))
                self.assertFalse(exists(fp_deleted))
            elif protocol == 'https':
                # as of 2025-09-26, I don't allow deletion of qiita main files
                # through API endpoints. Thus, the file is NOT deleted!
                # local version of the file
                self.assertTrue(exists(fp_test))
                # qiita main filepath
                self.assertTrue(exists(fp_obs))
                # qiita main filepath, returned by delete_file_from_central
                self.assertTrue(exists(fp_deleted))

    def test_fetch_directory(self):
        # creating a test directory
        fp_test = join('./job', '2_test_folder', 'source')
        self._create_test_dir(prefix=fp_test)

        # transmitting test directory into qiita main
        self.tester._plugincoupling = 'https'
        self.tester.push_file_to_central(fp_test)
        # a bit hacky, but should work as long as test database does not change
        ainfo = self.qclient.get('/qiita_db/artifacts/1/')
        base_data_dir = ainfo['files']['raw_forward_seqs'][0]['filepath'][
            :(-1 * len('raw_data/1_s_G1_L001_sequences.fastq.gz'))]
        fp_main = join(base_data_dir, join(*Path(fp_test).parts))

        # fetch test directory from qiita main, this time storing it at
        # QIITA_BASE_DIR
        prefix = join(expanduser("~"), 'localFetch')
        fp_obs = self.tester.fetch_file_from_central(
            dirname(fp_main), prefix=prefix)

        # test a file of the freshly transferred directory from main has
        # expected file content
        with open(join(fp_obs, 'job/2_test_folder/job/2_test_folder/',
                       'source', 'testdir', 'fileA.txt'), 'r') as f:
            self.assertIn('contentA', '\n'.join(f.readlines()))


if __name__ == '__main__':
    main()
