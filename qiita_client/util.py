# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

import pandas as pd

from subprocess import Popen, PIPE


def system_call(cmd):
    """Call command and return (stdout, stderr, return_value)

    Parameters
    ----------
    cmd : str or iterator of str
        The string containing the command to be run, or a sequence of strings
        that are the tokens of the command.

    Returns
    -------
    str, str, int
        - The standard output of the command
        - The standard error of the command
        - The exit status of the command

    Notes
    -----
    This function is ported from QIIME (http://www.qiime.org), previously named
    qiime_system_call. QIIME is a GPL project, but we obtained permission from
    the authors of this function to port it to Qiita and keep it under BSD
    license.
    """
    proc = Popen(cmd, universal_newlines=True, shell=True, stdout=PIPE,
                 stderr=PIPE)
    # Communicate pulls all stdout/stderr from the PIPEs
    # This call blocks until the command is done
    stdout, stderr = proc.communicate()
    return_value = proc.returncode
    return stdout, stderr, return_value


def get_sample_names_by_run_prefix(mapping_file):
    """Generates a dictionary of run_prefix and sample names

    Parameters
    ----------
    mapping_file : str
        The mapping file

    Returns
    -------
    dict
        Dict mapping run_prefix to sample id

    Raises
    ------
    ValueError
        If there is more than 1 sample per run_prefix
    """

    qiime_map = pd.read_csv(mapping_file, delimiter='\t', dtype=str,
                            encoding='utf-8', keep_default_na=False,
                            na_values=[])
    qiime_map.set_index('#SampleID', inplace=True)

    samples = {}
    errors = []
    for prefix, df in qiime_map.groupby('run_prefix'):
        len_df = len(df)
        if len_df != 1:
            errors.append('%s has %d samples (%s)' % (prefix, len_df,
                                                      ', '.join(df.index)))
        else:
            samples[prefix] = df.index.values[0]

    if errors:
        raise ValueError("You have run_prefix values with multiple "
                         "samples: %s" % ' -- '.join(errors))

    return samples
