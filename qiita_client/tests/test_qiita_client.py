# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import TestCase, main
from os import remove, close
from os.path import basename, exists
from tempfile import mkstemp
from json import dumps
import pandas as pd

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
        obs = self.tester.patch(f'/qiita_db/artifacts/{artifact_id}/', 'add',
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


if __name__ == '__main__':
    main()
