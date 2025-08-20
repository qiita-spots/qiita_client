# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

import traceback
import sys
from string import ascii_letters, digits
from random import SystemRandom
from os.path import exists, join, expanduser
from os import makedirs, environ
from future import standard_library
from json import dumps
import urllib
from qiita_client import QiitaClient

import logging

logger = logging.getLogger(__name__)

with standard_library.hooks():
    from configparser import ConfigParser


class QiitaCommand(object):
    """A plugin command

    Parameters
    ----------
    name : str
        The command name
    description : str
        The command description
    function : callable
        The function that executes the command. Should be a callable that
        conforms to the signature:
        `(bool, str, [ArtifactInfo] = function(qclient, job_id, job_parameters,
                                               output_dir, archive)`
        where qclient is an instance of QiitaClient, job_id is a string with
        the job identifier, job_parameters is a dictionary with the parameters
        of the command and output_dir is a string with the output directory.
        The function should return a boolean indicating if the command was
        executed successfully or not, a string containing a message in case
        of error, and a list of ArtifactInfo objects in case of success.
    required_parameters : dict of {str: (str, list of str)}
        The required parameters of the command, keyed by parameter name. The
        values should be a 2-tuple in which the first element is the parameter
        type, and the second parameter is the list of subtypes (if applicable)
    optional_parameters : dict of {str: (str, str)}
        The optional parameters of the command, keyed by parameter name. The
        values should be a 2-tuple in which the first element is the parameter
        name, and the second parameter is the default value
    outputs : dict of {str: str}
        The description of the outputs that this command generated. The
        format is: {output_name: artifact_type}
    default_parameter_sets : dict of {str: dict of {str: str}}
        The default parameter sets of the command, keyed by parameter set name.
        The values should be a dictionary in which keys are the parameter names
        and values are the specific value for each parameter
    analysis_only : bool, optional
        If true, the command will only be available on the analysis pipeline.
        Default: False

    Raises
    ------
    TypeError
        If `function` is not callable
    ValueError
        If `function` does not accept 4 parameters
    """
    def __init__(self, name, description, function, required_parameters,
                 optional_parameters, outputs, default_parameter_sets=None,
                 analysis_only=False):
        logger.debug('Entered QiitaCommand.__init__()')
        self.name = name
        self.description = description

        # Make sure that `function` is callable
        if not callable(function):
            raise TypeError(
                "Couldn't create command '%s': the provided function is not "
                "callable (type: %s)" % (name, type(function)))

        # `function` will be called with the following Parameters
        # qclient, job_id, job_parameters, output_dir
        # Make sure that `function` can receive 4 parameters
        if function.__code__.co_argcount != 4:
            raise ValueError(
                "Couldn't register command '%s': the provided function does "
                "not accept 4 parameters (number of parameters: %d)"
                % (name, function.__code__.co_argcount))

        self.function = function
        self.required_parameters = required_parameters
        self.optional_parameters = optional_parameters
        self.default_parameter_sets = default_parameter_sets
        self.outputs = outputs
        self.analysis_only = analysis_only

    def __call__(self, qclient, server_url, job_id, output_dir):
        logger.debug('Entered QiitaCommand.__call__()')
        return self.function(qclient, server_url, job_id, output_dir)


class QiitaArtifactType(object):
    """A Qiita artifact type

        Parameters
        ----------
        name : str
            The artifact type name
        description : str
            The artifact type description
        can_be_submitted_to_ebi : bool
            Whether the artifact type can be submitted to EBI or not
        can_be_submitted_to_vamps : bool
            Whether the artifact type can be submitted to VAMPS or not
        is_user_uploadable : bool
            Whether the artifact type can be uploaded directly by users
        filepath_types : list of (str, bool)
            The list filepath types that the new artifact type supports, and
            if they're required or not in an artifact instance of this type"""
    def __init__(self, name, description, can_be_submitted_to_ebi,
                 can_be_submitted_to_vamps, is_user_uploadable,
                 filepath_types):
        logger.debug('Entered QiitaArtifactType.__init__()')
        self.name = name
        self.description = description
        self.ebi = can_be_submitted_to_ebi
        self.vamps = can_be_submitted_to_vamps
        self.is_user_uploadable = is_user_uploadable
        self.fp_types = filepath_types


class BaseQiitaPlugin(object):
    # default must be first element
    _ALLOWED_PLUGIN_COUPLINGS = ['filesystem', 'https']

    def __init__(self, name, version, description, publications=None,
                 plugincoupling=_ALLOWED_PLUGIN_COUPLINGS[0]):
        logger.debug('Entered BaseQiitaPlugin.__init__()')
        self.name = name
        self.version = version
        self.description = description
        self.publications = dumps(publications) if publications else ""

        # Depending on your compute architecture, there are multiple options
        # available how "thight" plugins are coupled to the central
        # Qiita master/workers
        # --- filesystem ---
        # The default scenario is "filesystem", i.e. plugins as well as
        # master/worker have unrestricted direct access to a shared filesystem,
        # e.g. a larger volume / directory, defined in the server configuration
        # as base_data_dir
        # --- https ---
        # A second scenario is that your plugins execute as independent jobs on
        # another machine, e.g. as docker containers or other cloud techniques.
        # Intentionally, you don't want to use a shared filesystem, but you
        # have to make sure necessary input files are provided to the
        # containerized plugin before execution and resulting files are
        # transfered back to the central Qiita master/worker. In this case,
        # files are pulled / pushed through functions
        # qiita_client.fetch_file_from_central and
        # qiita_client.push_file_to_central, respectivey.
        # Actually, all files need to be decorated with this function.
        # The decision how data are transferred is then made within these two
        # functions according to the "plugincoupling" setting.
        if plugincoupling not in self._ALLOWED_PLUGIN_COUPLINGS:
            raise ValueError(
                ("valid plugincoupling values are ['%s'], but you "
                 "provided %s") % (
                     "', '".join(self._ALLOWED_PLUGIN_COUPLINGS),
                     plugincoupling))
        self.plugincoupling = plugincoupling

        # Will hold the different commands
        self.task_dict = {}

        # The configuration file
        conf_dir = environ.get(
            'QIITA_PLUGINS_DIR', join(expanduser('~'), '.qiita_plugins'))
        self.conf_fp = join(conf_dir, "%s_%s.conf" % (self.name, self.version))

    def generate_config(self, env_script, start_script, server_cert=None,
                        plugin_coupling=_ALLOWED_PLUGIN_COUPLINGS[0]):
        """Generates the plugin configuration file

        Parameters
        ----------
        env_script : str
            The CLI call used to load the environment in which the plugin is
            installed
        start_script : str
            The script used to start the plugin
        server_cert : str, optional
            If the Qiita server used does not have a valid certificate, the
            path to the Qiita certificate so the plugin can connect over
            HTTPS to it
        plugin_coupling : str
            Type of coupling of plugin to central for file exchange.
            Valid values are 'filesystem' and 'https'.
        """
        logger.debug('Entered BaseQiitaPlugin.generate_config()')
        sr = SystemRandom()
        chars = ascii_letters + digits
        client_id = ''.join(sr.choice(chars) for i in range(50))
        client_secret = ''.join(sr.choice(chars) for i in range(255))

        server_cert = server_cert if server_cert else ""

        with open(self.conf_fp, 'w') as f:
            f.write(CONF_TEMPLATE % (self.name, self.version, self.description,
                                     env_script, start_script,
                                     self._plugin_type, self.publications,
                                     server_cert, client_id, client_secret,
                                     plugin_coupling))

    def _register_command(self, command):
        """Registers a command in the plugin

        Parameters
        ----------
        command: QiitaCommand
            The command to be added to the plugin
        """
        logger.debug('Entered BaseQiitaPlugin._register_command(%s)' %
                     command.name)
        self.task_dict[command.name] = command

    def _register(self, qclient):
        """Registers the plugin information in Qiita"""
        # Get the command information from qiita
        logger.debug('Entered BaseQiitaPlugin._register()')
        info = qclient.get('/qiita_db/plugins/%s/%s/'
                           % (self.name, self.version))

        for cmd in self.task_dict.values():
            if cmd.name in info['commands']:
                qclient.post('/qiita_db/plugins/%s/%s/commands/%s/activate/'
                             % (self.name, self.version,
                                urllib.parse.quote(cmd.name)))
            else:
                req_params = {
                    k: v if v[0] != 'artifact' else ['artifact:%s'
                                                     % dumps(v[1]), None]
                    for k, v in cmd.required_parameters.items()}

                data = {'name': cmd.name,
                        'description': cmd.description,
                        'required_parameters': dumps(req_params),
                        'optional_parameters': dumps(cmd.optional_parameters),
                        'default_parameter_sets': dumps(
                            cmd.default_parameter_sets),
                        'outputs': dumps(cmd.outputs),
                        'analysis_only': cmd.analysis_only}
                qclient.post('/qiita_db/plugins/%s/%s/commands/'
                             % (self.name, self.version), data=data)

    def __call__(self, server_url, job_id, output_dir):
        """Runs the plugin and executed the assigned task

        Parameters
        ----------
        server_url : str
            The url of the server
        job_id : str
            The job id
        output_dir : str
            The output directory

        Raises
        ------
        RuntimeError
            If there is a problem gathering the job information
        """
        logger.debug('Entered BaseQiitaPlugin.__call__()')
        # Set up the Qiita Client
        config = ConfigParser()
        with open(self.conf_fp, 'U') as conf_file:
            config.readfp(conf_file)

        qclient = QiitaClient(server_url, config.get('oauth2', 'CLIENT_ID'),
                              config.get('oauth2', 'CLIENT_SECRET'),
                              # for this group of tests, confirm optional
                              # ca_cert parameter works as intended. Setting
                              # this value will prevent underlying libraries
                              # from validating the server's cert using
                              # certifi's pem cache.
                              ca_cert=config.get('oauth2', 'SERVER_CERT'),
                              plugincoupling=config.get('network',
                                                        'PLUGINCOUPLING'))

        if job_id == 'register':
            self._register(qclient)
        else:
            # Request job information. If there is a problem retrieving the job
            # information, the QiitaClient already raises an error
            job_info = qclient.get_job_info(job_id)
            # Starting the heartbeat
            qclient.start_heartbeat(job_id)
            # Execute the given task
            task_name = job_info['command']
            task = self.task_dict[task_name]

            if not exists(output_dir):
                makedirs(output_dir)
            try:
                success, artifacts_info, error_msg = task(
                    qclient, job_id, job_info['parameters'], output_dir)
            except Exception:
                exc_str = repr(traceback.format_exception(*sys.exc_info()))
                error_msg = ("Error executing %s:\n%s" % (task_name, exc_str))
                success = False
                artifacts_info = None
            # The job completed
            qclient.complete_job(job_id, success, error_msg=error_msg,
                                 artifacts_info=artifacts_info)


class QiitaTypePlugin(BaseQiitaPlugin):
    """Represents a Qiita Type Plugin

    Parameters
    ----------
    name : str
        The plugin name
    version : str
        The plugin version
    description : str
        The plugin description
    validate_func : callable
        The function used to validate artifacts
    html_generator_func : callable
        The function used to generate the HTML generator
    artifact_types : list of QiitaArtifactType
        The artifact types defined in this plugin

    Notes
    -----
    Both `validate_func` and `html_generator_func` should be a callable
    that conforms to the signature:
    `(bool, str, [ArtifactInfo] = function(qclient, job_id, job_parameters,
                                           output_dir, archive)`
    where qclient is an instance of QiitaClient, job_id is a string with
    the job identifier, job_parameters is a dictionary with the parameters
    of the command and output_dir is a string with the output directory.
    The function should return a boolean indicating if the command was
    executed successfully or not, a string containing a message in case
    of error, and a list of ArtifactInfo objects in case of success.
    """
    _plugin_type = "artifact definition"

    def __init__(self, name, version, description, validate_func,
                 html_generator_func, artifact_types, publications=None,
                 plugincoupling=BaseQiitaPlugin._ALLOWED_PLUGIN_COUPLINGS[0]):
        super(QiitaTypePlugin, self).__init__(name, version, description,
                                              publications=publications,
                                              plugincoupling=plugincoupling)

        logger.debug('Entered QiitaTypePlugin.__init__()')
        self.artifact_types = artifact_types

        val_cmd = QiitaCommand(
            'Validate', 'Validates a new artifact', validate_func,
            {'template': ('prep_template', None),
             'analysis': ('analysis', None),
             'files': ('string', None),
             'artifact_type': ('string', None)}, {}, None)

        self._register_command(val_cmd)

        html_cmd = QiitaCommand(
            'Generate HTML summary', 'Generates the HTML summary',
            html_generator_func,
            {'input_data': ('artifact',
                            [a.name for a in self.artifact_types])}, {}, None)

        self._register_command(html_cmd)

    def _register(self, qclient):
        """Registers the plugin information in Qiita"""
        logger.debug('Entered QiitaTypePlugin._register()')
        for at in self.artifact_types:
            data = {'type_name': at.name,
                    'description': at.description,
                    'can_be_submitted_to_ebi': at.ebi,
                    'can_be_submitted_to_vamps': at.vamps,
                    'is_user_uploadable': at.is_user_uploadable,
                    'filepath_types': dumps(at.fp_types)}
            qclient.post('/qiita_db/artifacts/types/', data=data)

        super(QiitaTypePlugin, self)._register(qclient)


class QiitaPlugin(BaseQiitaPlugin):
    """Represents a Qiita Plugin"""

    _plugin_type = "artifact transformation"

    def register_command(self, command):
        """Registers a command in the plugin

        Parameters
        ----------
        command: QiitaCommand
            The command to be added to the plugin
        """
        logger.debug('Entered QiitaPlugin.register_command()')
        self._register_command(command)


CONF_TEMPLATE = """[main]
NAME = %s
VERSION = %s
DESCRIPTION = %s
ENVIRONMENT_SCRIPT = %s
START_SCRIPT = %s
PLUGIN_TYPE = %s
PUBLICATIONS = %s

[oauth2]
SERVER_CERT = %s
CLIENT_ID = %s
CLIENT_SECRET = %s

[network]
PLUGINCOUPLING = %s
"""
