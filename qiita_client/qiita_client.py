# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

import time
import requests
import threading
import pandas as pd
from json import dumps
from random import randint

try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest

from os.path import basename

from .exceptions import (QiitaClientError, NotFoundError, BadRequestError,
                         ForbiddenError)

import logging

logger = logging.getLogger(__name__)

JOB_COMPLETED = False
# if the log level is not CRITICAL, the default, then we not expect slow
# responses from the server so let's make the retries values small
if logger.level != logging.CRITICAL:
    MAX_RETRIES = 1
    MIN_TIME_SLEEP = 1
    MAX_TIME_SLEEP = 3
else:
    MAX_RETRIES = 3
    MIN_TIME_SLEEP = 180
    MAX_TIME_SLEEP = 360
BLANK_FILE_THRESHOLD = 100


class ArtifactInfo(object):
    """Output artifact information

    Parameters
    ----------
    output_name : str
        The command's output name
    artifact_type : str
        Qiita's artifact type
    files : list of (str, str)
        The list of (filepath, Qiita's filepath type) that form the artifact
    archive : dict of {str: str}, optional
        A dict of features and their values to store. Format: {feature: values}
    """
    def __init__(self, output_name, artifact_type, files, archive=None):
        logger.debug('Entered ArtifactInfo.__init__()')
        self.output_name = output_name
        self.artifact_type = artifact_type
        self.files = files
        self.archive = archive if archive is not None else {}

    def __eq__(self, other):
        logger.debug('Entered ArtifactInfo.__eq__()')
        if type(self) is not type(other):
            return False
        if self.output_name != other.output_name or \
                self.artifact_type != other.artifact_type or \
                set(self.files) != set(other.files) or \
                self.archive != other.archive:
            return False
        return True

    def __ne__(self, other):
        logger.debug('Entered ArtifactInfo.__ne__()')
        return not self.__eq__(other)


def _heartbeat(qclient, url):
    """Send the heartbeat calls to the server

    Parameters
    ----------
    qclient : tgp.qiita_client.QiitaClient
        The Qiita server client
    url : str
        The url to issue the heartbeat

    Notes
    -----
    If the Qiita server is not reachable, this function will wait a random
    interval of minutes before retrying another heartbeat. This attempts to
    distribute the load of multiple callers during times of heavier load.
    Intervals are useful for updating the Qiita server without stopping long
    running jobs.
    """
    logger.debug('Entered _heartbeat()')
    retries = MAX_RETRIES
    while not JOB_COMPLETED and retries > 0:
        try:
            qclient.post(url, data='')
            retries = MAX_RETRIES
        except requests.ConnectionError:
            # This error occurs when the Qiita server is not reachable. This
            # may occur when we are updating the server, and we don't want
            # the job to fail. In this case, we wait for 5 min and try again
            stime = randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP)
            logger.debug('retry _heartbeat() %d, %d, %s' % (
                retries, stime, url))
            time.sleep(stime)
            retries -= 1
        except QiitaClientError:
            # If we raised the error, we propagate it since it is a problem
            # with the request that we are executing
            raise
        except Exception as e:
            # If it is any other exception, raise a RuntimeError
            raise RuntimeError("Error executing heartbeat: %s" % str(e))

        # Perform the heartbeat every 30 seconds
        time.sleep(30)


def _format_payload(success, error_msg=None, artifacts_info=None):
    """Generates the payload dictionary for the job

    Parameters
    ----------
    success : bool
        Whether if the job completed successfully or not
    error_msg : str, optional
        If `success` is False, ther error message to include in the optional.
        If `success` is True, it is ignored
    artifacts_info : list of ArtifactInfo, optional
        The list of output artifact information

    Returns
    -------
    dict
        Format:
        {'success': bool,
         'error': str,
         'artifacts': dict of {str: {'artifact_type': str,
                                     'filepaths': list of (str, str),
                                     'archive': {str: str}}}
    """
    logger.debug('Entered _format_payload()')
    if success and artifacts_info:
        artifacts = {
            a_info.output_name: {'artifact_type': a_info.artifact_type,
                                 'filepaths': a_info.files,
                                 'archive': a_info.archive}
            for a_info in artifacts_info}
    else:
        artifacts = None

    payload = {'success': success,
               'error': error_msg if not success else '',
               'artifacts': artifacts}
    return payload


class QiitaClient(object):
    """Client of the Qiita REST API

    Parameters
    ----------
    server_url : str
        The url of the Qiita server
    client_id : str
        The client id to connect to the Qiita server
    client_secret : str
        The client secret id to connect to the Qiita server
    ca_cert : str, optional
        CA cert used to sign and verify cert@server_url


    Methods
    -------
    get
    post
    """
    def __init__(self, server_url, client_id, client_secret, ca_cert=None):
        self._server_url = server_url
        self._session = requests.Session()

        # The attribute self._verify is used to provide the parameter `verify`
        # to the get/post requests. According to their documentation (link:
        # http://docs.python-requests.org/en/latest/user/
        # advanced/#ssl-cert-verification) verify can be a boolean indicating
        # if certificate verification should be performed or not, or a
        # string with the path to the certificate file that needs to be used
        # to verify the identity of the server.
        # We are setting this attribute at __init__ time to avoid executing
        # this if statement for each request issued.

        # As self-signed server certs are no longer allowed in one or more of
        # our dependencies, ca_cert (if provided) must now reference a file
        # that can be used to verify the certificate used by the server
        # referenced by server_url, rather than the server's own certificate.
        if not ca_cert:
            # The server certificate is not provided, use standard certificate
            # verification methods
            self._verify = True
        else:
            # The server certificate is provided, use it to verify the identity
            # of the server
            self._verify = ca_cert

        # Set up oauth2
        self._client_id = client_id
        self._client_secret = client_secret
        self._authenticate_url = "%s/qiita_db/authenticate/" % self._server_url

        # Fetch the access token
        self._token = None
        self._fetch_token()

    def _fetch_token(self):
        """Retrieves an access token from the Qiita server

        Raises
        ------
        ValueError
            If the authentication with the Qiita server fails
        """
        logger.debug('Entered QiitaClient._fetch_token()')
        data = {'client_id': self._client_id,
                'client_secret': self._client_secret,
                'grant_type': 'client'}

        logger.debug('data = %s' % data)
        logger.debug('authenticate_url = %s' % self._authenticate_url)
        logger.debug('verify = %s' % self._verify)
        try:
            r = self._session.post(self._authenticate_url,
                                   verify=self._verify,
                                   data=data, timeout=80)

            logger.debug('status code = %d' % r.status_code)

            if r.status_code != 200:
                raise ValueError("_fetchToken() POST request failed")

            logger.debug('RESULT.JSON() = %s' % r.json())
            self._token = r.json()['access_token']
            logger.debug('access_token = %s' % self._token)

        except Exception as e:
            # catches all errors including SSLError, ConnectionError,
            # Timeout, etc. and logs them
            logger.debug(str(e))

    def _request_oauth2(self, req, *args, **kwargs):
        """Executes a request using OAuth2 authorization

        Parameters
        ----------
        req : function
            The request to execute
        args : tuple
            The request args
        kwargs : dict
            The request kwargs

        Returns
        -------
        requests.Response
            The request response
        """
        logger.debug('Entered QiitaClient._request_oauth2()')
        if 'headers' in kwargs:
            kwargs['headers']['Authorization'] = 'Bearer %s' % self._token
        else:
            kwargs['headers'] = {'Authorization': 'Bearer %s' % self._token}

        # in case the Qiita server is not reachable (workers are busy), let's
        # try for 3 times with a 30 sec sleep between tries
        retries = MAX_RETRIES
        while retries >= 0:
            try:
                r = req(*args, **kwargs)
                r.close()
                break
            except requests.ConnectionError:
                if retries <= 0:
                    raise
                stime = randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP)
                logger.debug(
                    'retry QiitaClient._request_oauth2() %d, %d' % (
                        retries, stime))
                time.sleep(stime)
                retries -= 1
        if r.status_code == 400:
            try:
                r_json = r.json()
            except ValueError:
                r_json = None

            if r_json and 'error_description' in r_json:
                if r_json['error_description'] == \
                        'Oauth2 error: token has timed out':
                    # The token expired - get a new one and re-try the request
                    self._fetch_token()
                    kwargs['headers']['Authorization'] = \
                        'Bearer %s' % self._token
                    r = req(*args, **kwargs)
        return r

    def _request_retry(self, req, url, **kwargs):
        """Executes a request retrying it 2 times in case of failure

        Parameters
        ----------
        req : function
            The request to execute
        url : str
            The url to access in the server
        kwargs : dict
            The request kwargs

        Returns
        -------
        dict or None
            The JSON information in the request response, if any

        Raises
        ------
        NotFoundError
            If the request returned a 404 error
        BadRequestError
            If the request returned a 400 error
        ForbiddenError
            If the request returned a 403 error
        RuntimeError
            If the request did not succeed due to unknown causes

        Notes
        -----
        After doing some research on the topic, there are multiple ways of
        engineering the number of times a request should be retried (multiple
        sources - most of them on RPC systems). A short summary of those are:
          1. Keep retrying indefinitely
          2. The probability of retrying a request is based on the number of
          retries already done, as well as the cost of a retry
          3. Retry just once

        Number 1 could create an infinite loop. Number 2 is too complex and
        the cost of retrying depends on the actual work that we are currently
        doing (which is unknown to the current function). We thus decided to
        implement 3, which is simple and allows to overcome simple
        communication problems.
        """
        logger.debug('Entered QiitaClient._request_retry()')
        url = self._server_url + url
        retries = MAX_RETRIES
        while retries > 0:
            retries -= 1
            r = self._request_oauth2(req, url, verify=self._verify, **kwargs)
            r.close()
            # There are some error codes that the specification says that they
            # shouldn't be retried
            if r.status_code == 404:
                raise NotFoundError(r.text)
            elif r.status_code == 403:
                raise ForbiddenError(r.text)
            elif r.status_code == 400:
                raise BadRequestError(r.text)
            elif r.status_code in (500, 405):
                raise RuntimeError(
                    "Request '%s %s' did not succeed. Status code: %d. "
                    "Message: %s" % (req.__name__, url, r.status_code, r.text))
            elif 0 <= (r.status_code - 200) < 100:
                try:
                    return r.json()
                except ValueError:
                    return None
            stime = randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP)
            logger.debug('retry QiitaClient._request_retry() %d, %d, %s' % (
                retries, stime, url))
            time.sleep(stime)

        raise RuntimeError(
            "Request '%s %s' did not succeed. Status code: %d. Message: %s"
            % (req.__name__, url, r.status_code, r.text))

    def get(self, url, **kwargs):
        """Execute a get request against the Qiita server

        Parameters
        ----------
        url : str
            The url to access in the server
        kwargs : dict
            The request kwargs

        Returns
        -------
        dict
            The JSON response from the server
        """
        logger.debug('Entered QiitaClient.get()')
        return self._request_retry(self._session.get, url, **kwargs)

    def post(self, url, **kwargs):
        """Execute a post request against the Qiita server

        Parameters
        ----------
        url : str
            The url to access in the server
        kwargs : dict
            The request kwargs

        Returns
        -------
        dict
            The JSON response from the server
        """
        logger.debug('Entered QiitaClient.post(%s)' % url)
        return self._request_retry(self._session.post, url, **kwargs)

    def patch(self, url, op, path, value=None, from_p=None, **kwargs):
        """Executes a JSON patch request against the Qiita server

        The PATCH request is performed using the JSON PATCH specification [1]_.

        Parameters
        ----------
        url : str
            The url to access in the server
        op : str, {'add', 'remove', 'replace', 'move', 'copy', 'test'}
            The operation to perform in the PATCH request
        path : str
            The target location within the endpoint in which the operation
            should be performed
        value : str, optional
            If `op in ['add', 'replace', 'test']`, the new value for the given
            path
        from_p : str, optional
            If `op in ['move', 'copy']`, the original path
        kwargs : dict
            The request kwargs

        Raises
        ------
        ValueError
            If `op` has one of the values ['add', 'replace', 'test'] and
            `value` is None
            If `op` has one of the values ['move', 'copy'] and `from_p` is None

        References
        ----------
        .. [1] JSON PATCH spec: https://tools.ietf.org/html/rfc6902

        Returns
        -------
        dict
            The JSON response from the server
        """
        logger.debug('Entered QiitaClient.patch()')
        if op in ['add', 'replace', 'test'] and value is None:
            raise ValueError(
                "Operation '%s' requires the paramater 'value'" % op)
        if op in ['move', 'copy'] and from_p is None:
            raise ValueError(
                "Operation '%s' requires the parameter 'from_p'" % op)

        data = {'op': op, 'path': path}
        if value is not None:
            data['value'] = value
        if from_p is not None:
            data['from'] = from_p

        # Add the parameter 'data' to kwargs. Note that if it already existed
        # it is ok to overwrite given that otherwise the call will fail and
        # we made sure that data is correctly formatted here
        kwargs['data'] = data

        return self._request_retry(self._session.patch, url, **kwargs)

    # The functions are shortcuts for common functionality that all plugins
    # need to implement.

    def http_patch(self, url, **kwargs):
        """Executes a HTTP patch request against the Qiita server

        The PATCH request is performed using the HTTP PATCH specification [1]_.

        Parameters
        ----------
        url : str
            The url to access in the server
        kwargs : dict
            The request kwargs

        Returns
        -------
        dict
            The JSON response from the server
        """
        logger.debug('Entered QiitaClient.http_patch()')
        return self._request_retry(self._session.patch, url, **kwargs)

    def start_heartbeat(self, job_id):
        """Create and start a thread that would send heartbeats to the server

        Parameters
        ----------
        job_id : str
            The job id
        """
        logger.debug('Entered QiitaClient.start_heartbeat()')
        url = "/qiita_db/jobs/%s/heartbeat/" % job_id
        # Execute the first heartbeat, since it is the one that sets the job
        # to a running state - so make sure that other calls to the job work
        # as expected
        self.post(url, data='')
        heartbeat_thread = threading.Thread(target=_heartbeat,
                                            args=(self, url))
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

    def get_job_info(self, job_id):
        """Retrieve the job information from the server

        Parameters
        ----------
        job_id : str
            The job id

        Returns
        -------
        dict
            The JSON response from the server with the job information
        """
        logger.debug('Entered QiitaClient.get_job_info()')
        return self.get("/qiita_db/jobs/%s" % job_id)

    def update_job_step(self, job_id, new_step, ignore_error=True):
        """Updates the current step of the job in the server

        Parameters
        ----------
        jon_id : str
            The job id
        new_step : str
            The new step
        ignore_error : bool
            Problems communicating w/Qiita will not raise an Error.
        """
        logger.debug('Entered QiitaClient.update_job_step()')
        json_payload = dumps({'step': new_step})
        try:
            self.post("/qiita_db/jobs/%s/step/" % job_id, data=json_payload)
        except BaseException as e:
            if ignore_error is False:
                raise e

    def complete_job(self, job_id, success, error_msg=None,
                     artifacts_info=None):
        """Stops the heartbeat thread and sends the job results to the server

        Parameters
        ----------
        job_id : str
            The job id
        success : bool
            Whether the job completed successfully or not
        error_msg : str, optional
            If `success` is False, the error message to include.
            If `success` is True, it is ignored
        artifacts_info : list of ArtifactInfo
            The list of output artifact information
        """
        logger.debug('Entered QiitaClient.complete_job()')
        # Stop the heartbeat thread
        global JOB_COMPLETED
        JOB_COMPLETED = True
        json_payload = dumps(_format_payload(success, error_msg=error_msg,
                                             artifacts_info=artifacts_info))
        # Create the URL where we have to post the results
        self.post("/qiita_db/jobs/%s/complete/" % job_id, data=json_payload)

    def artifact_and_preparation_files(self, artifact_id,
                                       ignore_small_files=True):
        """Gets the artifact and preparation files from a given artifact_id

        Parameters
        ----------
        artifact_id : int
            The artifact id
        ignore_small_files : bool
            Whether to ignore small files or retrieve all of them (only applies
            to per_sample_FASTQ artifacts)

        Returns
        -------
        dict
            files available in the artifact
        pandas.DataFrame
            the prep information file for that artifact

        Raises
        ------
        RuntimeError
            - If the artifact belongs to an analysis

        """
        artifact_info = self.get("/qiita_db/artifacts/%s/" % artifact_id)

        if artifact_info['analysis'] is not None:
            raise RuntimeError(
                'Artifact ' + str(artifact_id) + ' is an analysis artifact, '
                'this method is meant to work with artifacts linked to '
                'a preparation.')

        prep_info = self.get('/qiita_db/prep_template/%s/'
                             % artifact_info['prep_information'][0])
        prep_info = pd.read_csv(prep_info['prep-file'], sep='\t', dtype=str)
        if artifact_info['type'] == 'per_sample_FASTQ':
            files, prep_info = self._process_files_per_sample_fastq(
                artifact_info['files'], prep_info, ignore_small_files)
        else:
            files = {k: [vv['filepath'] for vv in v]
                     for k, v in artifact_info['files'].items()}

        return files, prep_info

    def _process_files_per_sample_fastq(self, files, prep_info,
                                        ignore_small_files):
        "helper function to process per_sample_FASTQ artifacts and their preps"

        fwds = sorted(files['raw_forward_seqs'], key=lambda x: x['filepath'])
        revs = []
        if 'raw_reverse_seqs' in files:
            revs = sorted(
                files['raw_reverse_seqs'], key=lambda x: x['filepath'])
            if len(fwds) != len(revs):
                raise ValueError('The fwd (' + str(len(fwds)) + ') and rev ('
                                 + str(len(revs)) + ') files should be of the '
                                 'same length')

        run_prefixes = prep_info['run_prefix'].to_dict()

        # make parirings
        sample_names = dict()
        used_prefixes = []
        for i, (fwd, rev) in enumerate(zip_longest(fwds, revs)):
            fwd_fn = basename(fwd['filepath'])
            file_smaller_than_min = fwd['size'] < BLANK_FILE_THRESHOLD

            # iterate over run prefixes and make sure only one matches
            run_prefix = None
            sample_name = None
            for sn, rp in run_prefixes.items():
                if fwd_fn.startswith(rp) and run_prefix is None:
                    run_prefix = rp
                    sample_name = sn
                elif fwd_fn.startswith(rp) and run_prefix is not None:
                    raise ValueError('Multiple run prefixes match this fwd '
                                     'read: %s' % fwd_fn)

            if run_prefix is None:
                raise ValueError(
                    'No run prefix matching this fwd read: %s' % fwd_fn)
            if run_prefix in used_prefixes:
                raise ValueError(
                    'Run prefix matches multiple fwd reads: %s' % run_prefix)
            used_prefixes.append(run_prefix)

            if rev is not None:
                # if we have reverse reads, make sure the matching pair also
                # matches the run prefix:
                rev_fn = basename(rev['filepath'])
                if not file_smaller_than_min:
                    file_smaller_than_min = rev['size'] < BLANK_FILE_THRESHOLD
                if not rev_fn.startswith(run_prefix):
                    raise ValueError(
                        'Reverse read does not match run prefix. run_prefix: '
                        '%s; files: %s / %s') % (run_prefix, fwd_fn, rev_fn)

            used_prefixes.append(run_prefix)

            if ignore_small_files and file_smaller_than_min:
                continue

            sample_names[sample_name] = (fwd, rev)

        prep_info = prep_info.filter(items=sample_names.keys(), axis=0)

        return sample_names, prep_info
