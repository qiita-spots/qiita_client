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

debug_levels_list = [('DEBUG', logging.DEBUG),
                     ('INFO', logging.INFO),
                     ('WARNING', logging.WARNING),
                     ('ERROR', logging.ERROR),
                     ('CRITICAL', logging.CRITICAL)]

# logging level can be set to one of five levels:
# logging.DEBUG     (lowest level, includes ALL messages)
# logging.INFO      (next lowest level)
# logging.WARNING   (self-explanatory)
# logging.ERROR     (self-explanatory)
# logging.CRITICAL  (self-explanatory)
if 'QIITA_CLIENT_DEBUG_LEVEL' in environ:
    s = environ['QIITA_CLIENT_DEBUG_LEVEL']
    m = [x for x in debug_levels_list if x[0] == s]
    if m:
        logger.setLevel(m[1])
    else:
        s = "%s is not a valid value for QIITA_CLIENT_DEBUG_LEVEL" % s
        raise ValueError(s)
else:
    logger.setLevel(logging.CRITICAL)


logger.debug('logging instantiated and configured')

__all__ = ["QiitaClient", "QiitaClientError", "NotFoundError",
           "BadRequestError", "ForbiddenError", "ArtifactInfo", "QiitaCommand",
           "QiitaPlugin", "QiitaTypePlugin", "QiitaArtifactType"]
