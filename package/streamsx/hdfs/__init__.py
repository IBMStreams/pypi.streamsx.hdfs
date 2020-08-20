# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Overview
++++++++

Provides functions to access files on HDFS.

Use this package with the following services on IBM Cloud:

  * `Streaming Analytics <https://www.ibm.com/cloud/streaming-analytics>`_
  * `Analytics Engine <https://www.ibm.com/cloud/analytics-engine>`_


Credentials
+++++++++++

"Analytics Engine" credentials are defined using service credentials JSON.

The mandatory JSON elements are "user", "password" and "webhdfs"::

    {
        "user": "<USER>"
        "password": "<PASSWORD>",
        "webhdfs": "https://<HOST>:<PORT>"
    }

If you are using HDFS server(s) different to the "Analytics Engine" service, 
then you can provide the  *configuration file* (``hdfs-site.xml`` or ``core-site.xml``) to configure the connection.

Sample
++++++

A simple hello world example of a Streams application writing string messages to
a file to HDFS. Scan for created file on HDFS and read the content::

    from streamsx.topology.topology import *
    from streamsx.topology.schema import CommonSchema, StreamSchema
    from streamsx.topology.context import submit
    import streamsx.hdfs as hdfs

    credentials = json.load(credentials_analytics_engine_service)

    topo = Topology('HDFSHelloWorld')

    to_hdfs = topo.source(['Hello', 'World!'])
    to_hdfs = to_hdfs.as_string()
   
    # Write a stream to HDFS
    hdfs.write(to_hdfs, credentials=credentials, file='/sample/hw.txt')

    scanned = hdfs.scan(topo, credentials=credentials, directory='/sample', init_delay=10)
    
    # read text file line by line
    r = hdfs.read(scanned, credentials=credentials)
    
    # print each line (tuple)
    r.print()

    submit('STREAMING_ANALYTICS_SERVICE', topo)
    # Use for IBM Streams including IBM Cloud Pak for Data
    # submit ('DISTRIBUTED', topo)

"""

__version__='1.5.7'

__all__ = ['HdfsDirectoryScan', 'HdfsFileSink', 'HdfsFileSource', 'HdfsFileCopy', 'download_toolkit', 'configure_connection', 'scan', 'read', 'write']
from streamsx.hdfs._hdfs import download_toolkit, configure_connection, scan, read, write, copy, HdfsDirectoryScan, HdfsFileSink, HdfsFileSource, HdfsFileCopy
