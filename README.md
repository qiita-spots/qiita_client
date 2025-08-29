Qiita Client
============

[![Build Status](https://github.com/qiita-spots/qiita_client/actions/workflows/qiita-ci.yml/badge.svg)](https://github.com/qiita-spots/qiita_client/actions/workflows/qiita-ci.yml)

Qiita (canonically pronounced *cheetah*) is an analysis environment for microbiome (and other "comparative -omics") datasets.

This package includes the Qiita Client utility library, a library to simplify the communication between the plugins and the Qiita server.

How to test this package?
-------------------------
In order to test the Qiita Client package, a local installation of Qiita should be running in test mode on the address `https://localhost:8383`, with the default test database created in Qiita's test suite.
Also, if Qiita is running with the default server SSL certificate, you need to export the variable `QIITA_ROOTCA_CERT` in your environment, so the Qiita Client can perform secure connections against the Qiita server:

```bash

export QIITA_ROOT_CA=<QIITA_INSTALL_PATH>/qiita_core/support_files/ci_rootca.crt
```

Configure for cloud computing
-----------------------------
In the default scenario, Qiita main and Qiita plugins are executed on the same
machines, maybe spread across a Slurm or other grid compute cluster, but main
and plugins have direct access to all files in `BASE_DATA_DIR`.

This can be different, if you set up Qiita within a cloud compute environment,
where main and plugins do **not** share one file system. In this case, input-
files must first be transferred from main to plugin, then plugin can do its
processing and resulting files must be transferred back to main, once
processing is finished. To achieve this, the qiita_client, as it is part of
each plugin, provides the two functions for this file transfer
`fetch_file_from_central` and `push_file_to_central`. According to
`self._plugincoupling`, these functions operate on different "protocols";
as of 2025-08-29, either "filesystem" or "https". Switch to **"https"** for
cloud environments, default is **filesystem**.

The plugin coupling protocoll can be set in three ways

    1. default is always "filesystem", i.e. _DEFAULT_PLUGIN_COUPLINGS
        This is to be downward compatible.
    2. the plugin configuration can hold a section 'network' with an
        option 'PLUGINCOUPLING'. For old config files, this might not
        (yet) be the case. Therefore, we are double checking existance
        of this section and parameter here.
    3. you can set the environment variable QIITA_PLUGINCOUPLING
        Precedence is 3, 2, 1, i.e. the environment variable overrides the
        other two ways.
