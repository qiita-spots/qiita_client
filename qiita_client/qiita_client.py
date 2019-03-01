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
from json import dumps
from random import randint

from .exceptions import (QiitaClientError, NotFoundError, BadRequestError,
                         ForbiddenError)

JOB_COMPLETED = False
MAX_RETRIES = 3
MIN_TIME_SLEEP = 180
MAX_TIME_SLEEP = 360


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
        if type(self) != type(other):
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
    If the Qiita server is not reachable, this function will wait 5 minutes
    before retrying another heartbeat. This is useful for updating the Qiita
    server without stopping long running jobs.
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
            time.sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
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
    server_cert : str, optional
        The server certificate, in case that it is not verified


    Methods
    -------
    get
    post
    """
    def __init__(self, server_url, client_id, client_secret, server_cert=None):
        self._server_url = server_url
        logger.debug('Entered QiitaClient.__init__()')

        # The attribute self._verify is used to provide the parameter `verify`
        # to the get/post requests. According to their documentation (link:
        # http://docs.python-requests.org/en/latest/user/
        # advanced/#ssl-cert-verification) verify can be a boolean indicating
        # if certificate verification should be performed or not, or a
        # string with the path to the certificate file that needs to be used
        # to verify the identity of the server.
        # We are setting this attribute at __init__ time so we can avoid
        # executing this if statement for each request issued.
        if not server_cert:
            # The server certificate is not provided, use standard certificate
            # verification methods
            self._verify = True
        else:
            # The server certificate is provided, use it to verify the identity
            # of the server
            self._verify = server_cert

        # Set up oauth2
        self._client_id = client_id
        self._client_secret = client_secret
        self._authenticate_url = "%s/qiita_db/authenticate/" % self._server_url

        # Fetch the access token
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
        r = requests.post(self._authenticate_url, verify=self._verify,
                          data=data)
        if r.status_code != 200:
            raise ValueError("Can't authenticate with the Qiita server")
        self._token = r.json()['access_token']

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
                time.sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
                retries -= 1
        if r.status_code == 400:
            try:
                r_json = r.json()
            except ValueError:
                r_json = None

            if r_json and r_json['error_description'] == \
                    'Oauth2 error: token has timed out':
                # The token expired - get a new one and re-try the request
                self._fetch_token()
                kwargs['headers']['Authorization'] = 'Bearer %s' % self._token
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
            elif r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    return None
            time.sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))

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
        return self._request_retry(requests.get, url, **kwargs)

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
        logger.debug('Entered QiitaClient.post()')
        return self._request_retry(requests.post, url, **kwargs)

    def patch(self, url, op, path, value=None, from_p=None, **kwargs):
        """Executes a patch request against the Qiita server

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

        return self._request_retry(requests.patch, url, **kwargs)

    # The functions are shortcuts for common functionality that all plugins
    # need to implement.

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

    def update_job_step(self, job_id, new_step):
        """Updates the current step of the job in the server

        Parameters
        ----------
        jon_id : str
            The job id
        new_step : str
            The new step
        """
        logger.debug('Entered QiitaClient.update_job_step()')
        json_payload = dumps({'step': new_step})
        self.post("/qiita_db/jobs/%s/step/" % job_id, data=json_payload)

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
