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

logger = logging.getLogger()
handler = logging.StreamHandler()
fmt_str = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
formatter = logging.Formatter(fmt_str)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

logger.debug('logging instantiated and configured')

__all__ = ["QiitaClient", "QiitaClientError", "NotFoundError",
           "BadRequestError", "ForbiddenError", "ArtifactInfo", "QiitaCommand",
           "QiitaPlugin", "QiitaTypePlugin", "QiitaArtifactType"]
