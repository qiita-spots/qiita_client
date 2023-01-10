Qiita Client
============

[![Build Status](https://github.com/qiita-spots/qiita_client/actions/workflows/qiita-ci.yml/badge.svg)](https://github.com/qiita-spots/qiita_client/actions/workflows/qiita-ci.yml)

Qiita (canonically pronounced *cheetah*) is an analysis environment for microbiome (and other "comparative -omics") datasets.

This package includes the Qiita Client utility library, a library to simplify the communication between the plugins and the Qiita server.

How to test this package?
-------------------------
In order to test the Qiita Client package, a local installation of Qiita should be running in test mode on the address `https://localhost:21174`, with the default test database created in Qiita's test suite.
Also, if Qiita is running with the default server SSL certificate, you need to export the variable `QIITA_SERVER_CERT` in your environment, so the Qiita Client can perform secure connections against the Qiita server:

```bash

export QIITA_SERVER_CERT=<QIITA_INSTALL_PATH>/qiita_core/support_files/server.crt
```
