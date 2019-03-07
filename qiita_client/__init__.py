# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from .exceptions import (QiitaClientError, NotFoundError, BadRequestError,
                         ForbiddenError)
from .qiita_client import QiitaClient, ArtifactInfo
from .plugin import (QiitaCommand, QiitaPlugin, QiitaTypePlugin,
                     QiitaArtifactType)

import logging
from os import environ

handler = logging.StreamHandler()
fmt_str = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
handler.setFormatter(logging.Formatter(fmt_str))
logger = logging.getLogger(__name__)
logger.addHandler(handler)

debug_levels_list = {'DEBUG': logging.DEBUG,
                     'INFO': logging.INFO,
                     'WARNING': logging.WARNING,
                     'ERROR': logging.ERROR,
                     'CRITICAL': logging.CRITICAL}

# logging level can be set to one of five levels:
# logging.DEBUG     (lowest level, includes ALL messages)
# logging.INFO      (next lowest level)
# logging.WARNING   (self-explanatory)
# logging.ERROR     (self-explanatory)
# logging.CRITICAL  (self-explanatory)
if 'QIITA_CLIENT_DEBUG_LEVEL' in environ:
    level = environ['QIITA_CLIENT_DEBUG_LEVEL']
    if level in debug_levels_list:
        logger.setLevel(debug_levels_list[level])
    else:
        raise ValueError(
            "%s is not a valid value for QIITA_CLIENT_DEBUG_LEVEL" % level)
    logger.debug('logging set to %s' % level)
else:
    logger.setLevel(logging.CRITICAL)
    logger.debug('logging set to CRITICAL')


__all__ = ["QiitaClient", "QiitaClientError", "NotFoundError",
           "BadRequestError", "ForbiddenError", "ArtifactInfo", "QiitaCommand",
           "QiitaPlugin", "QiitaTypePlugin", "QiitaArtifactType"]
