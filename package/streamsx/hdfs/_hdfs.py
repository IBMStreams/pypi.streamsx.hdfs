# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import datetime
import json
import os.path
from os import path
from urllib.parse import urlparse
from enum import Enum

import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.toolkits import download_toolkit
import streamsx.topology.composite




_TOOLKIT_NAME = 'com.ibm.streamsx.hdfs'

FileInfoSchema = StreamSchema('tuple<rstring fileName, uint64 fileSize>')
"""Structured schema of the file write response tuple. This schema is the output schema of the write method.

``'tuple<rstring fileName, uint64 fileSize>'``
"""

FileCopySchema = StreamSchema('tuple<rstring message, uint64 elapsedTime>')
"""Structured schema of the file copy response tuple. This schema is the output schema of the copy method.

``'tuple<rstring message, uint64 elapsedTime>'``
"""

DirectoryScanSchema = StreamSchema('tuple<rstring fileName>')
"""Structured schema of the directory scan response tuple. This schema is the output schema of the scan method.

``'tuple<rstring fileName>'``
"""

def _add_toolkit_dependency(topo):
    # IMPORTANT: Dependency of this python wrapper to a specific toolkit version
    # This is important when toolkit is not set with streamsx.spl.toolkit.add_toolkit (selecting toolkit from remote build service)
    streamsx.spl.toolkit.add_toolkit_dependency(topo, _TOOLKIT_NAME, '[5.0.0,6.0.0)')

def _read_service_credentials(credentials):
    hdfs_uri = ""
    user = ""
    password = ""
    if isinstance(credentials, dict):
        # check for Analytics Engine service credentials
        if 'cluster' in credentials:
            user = credentials.get('cluster').get('user')
            password = credentials.get('cluster').get('password')
            hdfs_uri = credentials.get('cluster').get('service_endpoints').get('webhdfs')
        else:
            if 'webhdfs' in credentials:
                user = credentials.get('user')
                password = credentials.get('password')
                hdfs_uri = credentials.get('webhdfs')
            else:
                raise ValueError(credentials)
    else:
         raise TypeError(credentials)
    # construct expected format for hdfs_uri: webhdfs://host:port
    uri_parsed = urlparse(hdfs_uri)
    hdfs_uri = 'webhdfs://'+uri_parsed.netloc
    return hdfs_uri, user, password

def _check_vresion_credentials1(credentials, _op, topology):
    if credentials is not None:
 
        # check streamsx.hdfs version
        _add_toolkit_dependency(topology)
        #    if credentials is None:
        #        raise SystemExit("Error: credentials is empty.")
        print( credentials)
        topology.stream.name
        print( '------------' + topology.stream.name + " "  + str(credentials))
        if isinstance(credentials, dict):
            hdfs_uri, user, password = _read_service_credentials(credentials)
            _op.params['hdfsUri'] = hdfs_uri
            _op.params['hdfsUser'] = user
            _op.params['hdfsPassword'] = password
        #check if the credentials is a valid JSON string
        elif _is_a_valid_json(credentials):
            _op.params['credentials'] = credentials
        else:    
            # expect core-site.xml file in credentials param 
            try:
                print( '--------------------------*******+++---' + str(credentials))
          #          with open(str(credentials)):
                print(credentials)
                topology.add_file_dependency(credentials, 'etc')
                _op.params['configPath'] = 'etc'
                _op.params['credentials'] = None
            except IOError:
                raise SystemExit("Error: File not accessible.")



def _setCredentials(LocalCredentials, topology):
    credentials=None
    hdfsUri=None
    hdfsUser=None
    hdfsPassword=None
    configPath=None
    if LocalCredentials is not None:
         # print( '-----  Credentials -----' + topology.name + " : "  + str(LocalCredentials))
         # check the streamsx.hdfs toolkit version           
         _add_toolkit_dependency(topology)     
         
         if ('xml' in LocalCredentials):
             try:
                  with open(LocalCredentials):
                      topology.add_file_dependency(LocalCredentials, 'etc')
                      configPath = 'etc'
                      credentials = None
             except IOError:
                raise ValueError(LocalCredentials)
         else:
             if isinstance(credentials, dict):
                 hdfsUri, hdfsUser, hdfsPassword = _read_service_credentials(LocalCredentials)
             else:
                credentials=LocalCredentials
  
    return credentials, hdfsUri, hdfsUser, hdfsPassword, configPath



   
def _check_time_param(time_value, parameter_name):
    if isinstance(time_value, datetime.timedelta):
        result = time_value.total_seconds()
    elif isinstance(time_value, int) or isinstance(time_value, float):
        result = time_value
    else:
        raise TypeError(time_value)
    if result <= 1:
        raise ValueError("Invalid "+parameter_name+" value. Value must be at least one second.")
    return result

def _is_a_valid_json(credentials):
    # checking if the input string is a valid JSON string  
    try: 
        json.loads(credentials) 
        return 1
    except:
        pass
        return 0
           
class CopyDirection(Enum):
    """Defines File Copy directions for HDFS2FileCopy.

    .. versionadded:: 1.2
    """  
    copyFromLocalFile = 0
    """Copy from local to HDFS 
    """

    copyToLocalFile = 1
    """Copy from HDFS to local 
    """

def _convert_copy_direction_string_to_enum(value):
    """convert string into enum value
    """  
    direction = CopyDirection(0)
    if value == 'copyFromLocalFile':
        direction = CopyDirection(0)    
    elif value == 'copyToLocalFile':
        direction = CopyDirection(1) 
    else:
        raise NotImplementedError("Copy Direction parsing not implemented: " + value)
    return direction


def configure_connection (instance, name = 'hdfs', credentials = None):
    """Configures IBM Streams for a certain connection.


    Creates or updates an application configuration object containing the required properties with connection information.


    Example for creating a configuration for a Streams instance with connection details::

        from streamsx.rest import Instance
        import streamsx.topology.context
        from icpd_core import icpd_util
        import streamsx.hdfs as hdfs
        
        cfg = icpd_util.get_service_instance_details (name='your-streams-instance')
        cfg[context.ConfigParams.SSL_VERIFY] = False
        instance = Instance.of_service (cfg)
        app_cfg = hdfs.configure_connection (instance, credentials = 'my_credentials_json')

    Args:
        instance(streamsx.rest_primitives.Instance): IBM Streams instance object.
        name(str): Name of the application configuration, default name is 'hdfs'.
        credentials(str|dict): The service credentials, for example Analytics Engine service credentials.
    Returns:
        Name of the application configuration.
    """

    description = 'HDFS credentials'
    properties = {}
    if credentials is None:
        raise TypeError (credentials)
    
    if isinstance (credentials, dict):
        properties ['credentials'] = json.dumps (credentials)
    else:
        properties ['credentials'] = credentials
    
    # check if application configuration exists
    app_config = instance.get_application_configurations (name = name)
    if app_config:
        print ('update application configuration: ' + name)
        app_config[0].update (properties)
    else:
        print ('create application configuration: ' + name)
        instance.create_application_configuration (name, properties, description)
    return name


def download_toolkit(url=None, target_dir=None):
    r"""Downloads the latest HDFS toolkit from GitHub.

    Example for updating the HDFS toolkit for your topology with the latest toolkit from GitHub::

        import streamsx.hdfs as hdfs
        # download HDFS toolkit from GitHub
        hdfs_toolkit_location = hdfs.download_toolkit()
        # add the toolkit to topology
        streamsx.spl.toolkit.add_toolkit(topology, hdfs_toolkit_location)

    Example for updating the topology with a specific version of the HDFS toolkit using a URL::

        import streamsx.hdfs as hdfs
        url500 = 'https://github.com/IBMStreams/streamsx.hdfs/releases/download/v5.0.0/streamx.hdfs.toolkits-5.0.0-20190902-1637.tgz'
        hdfs_toolkit_location = hdfs.download_toolkit(url=url500)
        streamsx.spl.toolkit.add_toolkit(topology, hdfs_toolkit_location)

    Args:
        url(str): Link to toolkit archive (\*.tgz) to be downloaded. Use this parameter to objectstorage
            download a specific version of the toolkit.
        target_dir(str): the directory where the toolkit is unpacked to. If a relative path is given,
            the path is appended to the system temporary directory, for example to /tmp on Unix/Linux systems.
            If target_dir is ``None`` a location relative to the system temporary directory is chosen.

    Returns:
        str: the location of the downloaded HDFS toolkit

    .. note:: This function requires an outgoing Internet connection
    .. versionadded:: 1.1
    """
    _toolkit_location = streamsx.toolkits.download_toolkit (toolkit_name=_TOOLKIT_NAME, url=url, target_dir=target_dir)
    return _toolkit_location

    

def scan(topology, credentials, directory, pattern=None, init_delay=None, name=None):
    """Scans a Hadoop Distributed File System directory for new or modified files.

    Repeatedly scans a HDFS directory and writes the names of new or modified files that are found in the directory to the output stream.

    Args:
        topology(Topology): Topology to contain the returned stream.
        credentials(dict|str|file): The credentials of the IBM cloud Analytics Engine service in *JSON* (idct) or JSON string (str) or the path to the *configuration file* (``hdfs-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        directory(str): The directory to be scanned. Relative path is relative to the '/user/userid/' directory. 
        pattern(str): Limits the file names that are listed to the names that match the specified regular expression.
        init_delay(int|float|datetime.timedelta): The time to wait in seconds before the operator scans the directory for the first time. If not set, then the default value is 0.
        schema(Schema): Optional output stream schema. Default is ``CommonSchema.String``. Alternative a structured streams schema with a single attribute of type ``rstring`` is supported.  
        name(str): Source name in the Streams context, defaults to a generated name.

    Returns:
        Output Stream containing file names with schema :py:const:`~streamsx.hdfs.DirectoryScanSchema`.
     """
    credentials, hdfsUri, hdfsUser, hdfsPassword, configPath = _setCredentials(credentials, topology)
    _op = _HDFS2DirectoryScan(topology, configPath=configPath, credentials=credentials, hdfsUri=hdfsUri, hdfsUser=hdfsUser,  hdfsPassword=hdfsPassword, directory=directory, pattern=pattern, schema=DirectoryScanSchema, name=name)

 #   _check_vresion_credentials(credentials, _op, topology)

    if init_delay is not None:
        _op.params['initDelay'] = streamsx.spl.types.float64(_check_time_param(init_delay, 'init_delay'))

    return _op.outputs[0]

def scanComposite(topology, credentials, directory, pattern=None, init_delay=None, name=None):
    """Scans a Hadoop Distributed File System directory for new or modified files.

    Repeatedly scans a HDFS directory and writes the names of new or modified files that are found in the directory to the output stream.

    Args:
        topology(Topology): Topology to contain the returned stream.
        credentials(dict|str|file): The credentials of the IBM cloud Analytics Engine service in *JSON* (idct) or JSON string (str) or the path to the *configuration file* (``hdfs-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        directory(str): The directory to be scanned. Relative path is relative to the '/user/userid/' directory. 
        pattern(str): Limits the file names that are listed to the names that match the specified regular expression.
        init_delay(int|float|datetime.timedelta): The time to wait in seconds before the operator scans the directory for the first time. If not set, then the default value is 0.
        schema(Schema): Optional output stream schema. Default is ``CommonSchema.String``. Alternative a structured streams schema with a single attribute of type ``rstring`` is supported.  
        name(str): Source name in the Streams context, defaults to a generated name.

    Returns:
        Output Stream containing file names with schema :py:const:`~streamsx.hdfs.DirectoryScanSchema`.
     """

    _op = HdfsDirectoryScan(topology, directory=directory, pattern=pattern, schema=DirectoryScanSchema, name=name)

#    _check_vresion_credentials(credentials, _op, topology)

#    if init_delay is not None:
#        _op.params['initDelay'] = streamsx.spl.types.float64(_check_time_param(init_delay, 'init_delay'))

    return _op.outputs[0]



def read(stream, credentials, schema=CommonSchema.String, name=None):
    """Reads files from a Hadoop Distributed File System.

    Filenames of file to be read are part of the input stream.

    Args:
        stream(Stream): Stream of tuples containing file names to be read. Supports ``CommonSchema.String`` as input. Alternative a structured streams schema with a single attribute of type ``rstring`` is supported.
        credentials(dict|str|file): The credentials of the IBM cloud Analytics Engine service in *JSON* (idct) or JSON string (str) or the path to the *configuration file* (``hdfs-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        schema(Schema): Output schema for the file content, defaults to ``CommonSchema.String``. Alternative a structured streams schema with a single attribute of type ``rstring`` or ``blob`` is supported.
        name(str): Name of the operator in the Streams context, defaults to a generated name.

    Returns:
        Output Stream for file content. Default output schema is ``CommonSchema.String`` (line per file).
    """

    credentials, hdfsUri, hdfsUser, hdfsPassword, configPath = _setCredentials(credentials, stream.topology)
    _op = _HDFS2FileSource(stream, configPath=configPath, credentials=credentials, hdfsUri=hdfsUri, hdfsUser=hdfsUser,  hdfsPassword=hdfsPassword, schema=schema, name=name)

#    _check_vresion_credentials(credentials, _op, stream.topology)

    return _op.outputs[0]


def write(stream, credentials, file=None, fileAttributeName=None, schema=None, timePerFile=None, tuplesPerFile=None, bytesPerFile=None, name=None):
    """Writes files to a Hadoop Distributed File System.

    When writing to a file, that exists already on HDFS with the same name, then this file is overwritten.
    Per default the file is closed when window punctuation mark is received. Different close modes can be specified with the parameters: ``timePerFile``, ``tuplesPerFile``, ``bytesPerFile``

    Example with input stream of type ``CommonSchema.String``::

        import streamsx.hdfs as hdfs
        
        s = topo.source(['Hello World!']).as_string()
        result = hdfs.write(s, credentials=credentials, file='sample%FILENUM.txt')
        result.print()

    Args:
        stream(Stream): Stream of tuples containing the data to be written to files. Supports ``CommonSchema.String`` as input. Alternative a structured streams schema with a single attribute of type ``rstring`` or ``blob`` is supported.
        credentials(dict|str|file): The credentials of the IBM cloud Analytics Engine service in *JSON* (idct) or JSON string (str) or the path to the *configuration file* (``hdfs-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        file(str): Specifies the name of the file. The file parameter can optionally contain the following variables, which are evaluated at runtime to generate the file name:
         
          * %FILENUM The file number, which starts at 0 and counts up as a new file is created for writing.
         
          * %TIME The time when the file is created. The time format is yyyyMMdd_HHmmss.
          
          Important: If the %FILENUM or %TIME specification is not included, the file is overwritten every time a new file is created.
        timePerFile(int|float|datetime.timedelta): Specifies the approximate time, in seconds, after which the current output file is closed and a new file is opened for writing. The ``bytesPerFile``, ``timePerFile`` and ``tuplesPerFile`` parameters are mutually exclusive.
        tuplesPerFile(int): The maximum number of tuples that can be received for each output file. When the specified number of tuples are received, the current output file is closed and a new file is opened for writing. The ``bytesPerFile``, ``timePerFile`` and ``tuplesPerFile`` parameters are mutually exclusive. 
        bytesPerFile(int): Approximate size of the output file, in bytes. When the file size exceeds the specified number of bytes, the current output file is closed and a new file is opened for writing. The ``bytesPerFile``, ``timePerFile`` and ``tuplesPerFile`` parameters are mutually exclusive.
        name(str): Sink name in the Streams context, defaults to a generated name.

    Returns:
        Output Stream with schema :py:const:`~streamsx.hdfs.FileInfoSchema`.
    """
    # check bytesPerFile, timePerFile and tuplesPerFile parameters
    if (timePerFile is not None and tuplesPerFile is not None) or (tuplesPerFile is not None and bytesPerFile is not None) or (timePerFile is not None and bytesPerFile is not None):
        raise ValueError("The parameters are mutually exclusive: bytesPerFile, timePerFile, tuplesPerFile")

    credentials, hdfsUri, hdfsUser, hdfsPassword, configPath = _setCredentials(credentials, stream.topology)
    _op = _HDFS2FileSink(stream, configPath=configPath, credentials=credentials, hdfsUri=hdfsUri, hdfsUser=hdfsUser,  hdfsPassword=hdfsPassword, file=file, fileAttributeName=fileAttributeName, schema=FileInfoSchema, name=name)

#    _check_vresion_credentials(credentials, _op, stream.topology)
    

    if timePerFile is None and tuplesPerFile is None and bytesPerFile is None:
        _op.params['closeOnPunct'] = _op.expression('true')
    if timePerFile is not None:
        _op.params['timePerFile'] = streamsx.spl.types.float64(_check_time_param(timePerFile, 'timePerFile'))
    if tuplesPerFile is not None:
        _op.params['tuplesPerFile'] = streamsx.spl.types.int64(tuplesPerFile)
    if bytesPerFile is not None:
        _op.params['bytesPerFile'] = streamsx.spl.types.int64(bytesPerFile)
    return _op.outputs[0]


def copy(stream, credentials, direction, hdfsFile=None, hdfsFileAttrName=None, localFile=None, name=None):
    """Copy a Hadoop Distributed File to local and copy a local file to te HDFS.

    Repeatedly scans a HDFS directory and writes the names of new or modified files that are found in the directory to the output stream.

    Args:
        topology(Topology): Topology to contain the returned stream.
        credentials(dict|str|file): The credentials of the IBM cloud Analytics Engine service in *JSON* (idct) or JSON string (str) or the path to the *configuration file* (``hdfs-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        direction(str): This mandatory parameter specifies the direction of copy. The parameter can be set with the following values. **'copyFromLocalFile'** Copy a file from local disk to the HDFS file system. **'copyToLocalFile'** Copy a file from HDFS file system to the local disk.
        hdfsFile(str): This parameter Specifies the name of HDFS file or directory. If the name starts with a slash, it is considered an absolute path of HDFS file that you want to use. If it does not start with a slash, it is considered a relative path, relative to the /user/userid/hdfsFile .
        localFile(str): This parameter specifies the name of local file to be copied. If the name starts with a slash, it is considered an absolute path of local file that you want to copy. If it does not start with a slash, it is considered a relative path, relative to your project data directory. 
        schema(Schema): Optional output stream schema. Default is ``CommonSchema.String``. Alternative a structured streams schema with a single attribute of type ``rstring`` is supported.  
        name(str): Source name in the Streams context, defaults to a generated name.

    Returns:
        Output Stream containing the result message and teh elapsed time with schema :py:const:`~streamsx.hdfs.FileCopySchema`.
    """

    Direction=_convert_copy_direction_string_to_enum(direction)
    
    credentials, hdfsUri, hdfsUser, hdfsPassword, configPath = _setCredentials(credentials, stream.topology)
    _op = _HDFS2FileCopy(stream, configPath=configPath, credentials=credentials, hdfsUri=hdfsUri, hdfsUser=hdfsUser,  hdfsPassword=hdfsPassword, direction=Direction, hdfsFileAttrName=hdfsFileAttrName, localFile=localFile , schema=FileCopySchema, name=name)

#    _check_vresion_credentials(credentials, _op, stream.topology)
    
    return _op.outputs[0]




class _HDFS2DirectoryScan(streamsx.spl.op.Source):
    def __init__(self, topology, schema, appConfigName=None, authKeytab=None, authPrincipal=None, configPath=None, credFile=None, credentials=None, directory=None, hdfsPassword=None, hdfsUri=None, hdfsUser=None, initDelay=None, keyStorePassword=None, keyStorePath=None, libPath=None, pattern=None, policyFilePath=None, reconnectionBound=None, reconnectionInterval=None, reconnectionPolicy=None, sleepTime=None, strictMode=None, vmArg=None, name=None):
        kind="com.ibm.streamsx.hdfs::HDFS2DirectoryScan"
 #       inputs=None
        schemas=schema
        params = dict()
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if configPath is not None:
            params['configPath'] = configPath
        if credFile is not None:
            params['credFile'] = credFile
        if credentials is not None:
            params['credentials'] = credentials
        if directory is not None:
            params['directory'] = directory
        if hdfsPassword is not None:
            params['hdfsPassword'] = hdfsPassword
        if hdfsUri is not None:
            params['hdfsUri'] = hdfsUri
        if hdfsUser is not None:
            params['hdfsUser'] = hdfsUser
        if initDelay is not None:
            params['initDelay'] = initDelay
        if keyStorePassword is not None:
            params['keyStorePassword'] = keyStorePassword
        if keyStorePath is not None:
            params['keyStorePath'] = keyStorePath
        if libPath is not None:
            params['libPath'] = libPath
        if pattern is not None:
            params['pattern'] = pattern
        if policyFilePath is not None:
            params['policyFilePath'] = policyFilePath
        if reconnectionBound is not None:
            params['reconnectionBound'] = reconnectionBound
        if reconnectionInterval is not None:
            params['reconnectionInterval'] = reconnectionInterval
        if reconnectionPolicy is not None:
            params['reconnectionPolicy'] = reconnectionPolicy
        if sleepTime is not None:
            params['sleepTime'] = sleepTime
        if strictMode is not None:
            params['strictMode'] = strictMode
        if vmArg is not None:
            params['vmArg'] = vmArg

        super(_HDFS2DirectoryScan, self).__init__(topology,kind,schemas,params,name)


class _HDFS2FileSource(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, appConfigName=None, authKeytab=None, authPrincipal=None, blockSize=None, configPath=None, credFile=None, 
                 credentials=None, encoding=None, file=None, hdfsPassword=None, hdfsUri=None, hdfsUser=None, initDelay=None, keyStorePassword=None, 
                 keyStorePath=None, libPath=None, policyFilePath=None, reconnectionBound=None, reconnectionInterval=None, reconnectionPolicy=None, vmArg=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hdfs::HDFS2FileSource"
        inputs=stream
#        schemas=schema
        params = dict()
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if blockSize is not None:
            params['blockSize'] = blockSize
        if configPath is not None:
            params['configPath'] = configPath
        if credFile is not None:
            params['credFile'] = credFile
        if credentials is not None:
            params['credentials'] = credentials
        if encoding is not None:
            params['encoding'] = encoding
        if file is not None:
            params['file'] = file
        if hdfsPassword is not None:
            params['hdfsPassword'] = hdfsPassword
        if hdfsUri is not None:
            params['hdfsUri'] = hdfsUri
        if hdfsUser is not None:
            params['hdfsUser'] = hdfsUser
        if initDelay is not None:
            params['initDelay'] = initDelay
        if keyStorePassword is not None:
            params['keyStorePassword'] = keyStorePassword
        if keyStorePath is not None:
            params['keyStorePath'] = keyStorePath
        if libPath is not None:
            params['libPath'] = libPath
        if policyFilePath is not None:
            params['policyFilePath'] = policyFilePath
        if reconnectionBound is not None:
            params['reconnectionBound'] = reconnectionBound
        if reconnectionInterval is not None:
            params['reconnectionInterval'] = reconnectionInterval
        if reconnectionPolicy is not None:
            params['reconnectionPolicy'] = reconnectionPolicy
        if vmArg is not None:
            params['vmArg'] = vmArg

        super(_HDFS2FileSource, self).__init__(topology,kind,inputs,schema,params,name)



class _HDFS2FileSink(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, appConfigName=None, authKeytab=None, authPrincipal=None, bytesPerFile=None, closeOnPunct=None, configPath=None, 
                 credFile=None, credentials=None, encoding=None, file=None, fileAttributeName=None, hdfsPassword=None, hdfsUri=None, hdfsUser=None, keyStorePassword=None, keyStorePath=None, libPath=None, policyFilePath=None, 
                 reconnectionBound=None, reconnectionInterval=None, reconnectionPolicy=None, tempFile=None, timeFormat=None, timePerFile=None, tuplesPerFile=None, vmArg=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hdfs::HDFS2FileSink"
        inputs=stream
 #       schemas=schema
        params = dict()
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if bytesPerFile is not None:
            params['bytesPerFile'] = bytesPerFile
        if closeOnPunct is not None:
            params['closeOnPunct'] = closeOnPunct
        if configPath is not None:
            params['configPath'] = configPath
        if credFile is not None:
            params['credFile'] = credFile
        if credentials is not None:
            params['credentials'] = credentials
        if encoding is not None:
            params['encoding'] = encoding
        if file is not None:
            params['file'] = file
        if fileAttributeName is not None:
            params['fileAttributeName'] = fileAttributeName
        if hdfsPassword is not None:
            params['hdfsPassword'] = hdfsPassword
        if hdfsUri is not None:
            params['hdfsUri'] = hdfsUri
        if hdfsUser is not None:
            params['hdfsUser'] = hdfsUser
        if keyStorePassword is not None:
            params['keyStorePassword'] = keyStorePassword
        if keyStorePath is not None:
            params['keyStorePath'] = keyStorePath
        if libPath is not None:
            params['libPath'] = libPath
        if policyFilePath is not None:
            params['policyFilePath'] = policyFilePath
        if reconnectionBound is not None:
            params['reconnectionBound'] = reconnectionBound
        if reconnectionInterval is not None:
            params['reconnectionInterval'] = reconnectionInterval
        if reconnectionPolicy is not None:
            params['reconnectionPolicy'] = reconnectionPolicy
        if tempFile is not None:
            params['tempFile'] = tempFile
        if timeFormat is not None:
            params['timeFormat'] = timeFormat
        if timePerFile is not None:
            params['timePerFile'] = timePerFile
        if tuplesPerFile is not None:
            params['tuplesPerFile'] = tuplesPerFile
        if vmArg is not None:
            params['vmArg'] = vmArg

        super(_HDFS2FileSink, self).__init__(topology,kind,inputs,schema,params,name)


class _HDFS2FileCopy(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, appConfigName=None, authKeytab=None, authPrincipal=None, configPath=None, credFile=None,  credentials=None, 
                 deleteSourceFile=None,  direction=None, hdfsFile=None, hdfsFileAttrName=None, hdfsPassword=None, hdfsUri=None, 
                 hdfsUser=None, keyStorePassword=None, keyStorePath=None, libPath=None, localFile=None, localFileAttrName =None, overwriteDestinationFile=None, 
                 policyFilePath=None, reconnectionBound=None, reconnectionInterval=None, reconnectionPolicy=None, vmArg=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hdfs::HDFS2FileCopy"
        inputs=stream
#        schemas=schema
        params = dict()
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if configPath is not None:
            params['configPath'] = configPath
        if credFile is not None:
            params['credFile'] = credFile
        if credentials is not None:
            params['credentials'] = credentials
        if deleteSourceFile is not None:
            params['deleteSourceFile'] = deleteSourceFile
        if direction is not None:
            params['direction'] = direction
        if hdfsFile is not None:
            params['hdfsFile'] = hdfsFile
        if hdfsFileAttrName is not None:
            params['hdfsFileAttrName'] = hdfsFileAttrName
        if hdfsPassword is not None:
            params['hdfsPassword'] = hdfsPassword
        if hdfsUri is not None:
            params['hdfsUri'] = hdfsUri
        if hdfsUser is not None:
            params['hdfsUser'] = hdfsUser
        if keyStorePassword is not None:
            params['keyStorePassword'] = keyStorePassword
        if keyStorePath is not None:
            params['keyStorePath'] = keyStorePath
        if libPath is not None:
            params['libPath'] = libPath
        if localFile is not None:
            params['localFile'] = localFile
        if localFileAttrName is not None:
            params['localFileAttrName'] = localFileAttrName
        if overwriteDestinationFile is not None:
            params['overwriteDestinationFile'] = overwriteDestinationFile
        if reconnectionBound is not None:
            params['reconnectionBound'] = reconnectionBound
        if reconnectionInterval is not None:
            params['reconnectionInterval'] = reconnectionInterval
        if reconnectionPolicy is not None:
            params['reconnectionPolicy'] = reconnectionPolicy
        if vmArg is not None:
            params['vmArg'] = vmArg

        super(_HDFS2FileCopy, self).__init__(topology,kind,inputs,schema,params,name)

class HdfsDirectoryScan(streamsx.topology.composite.Source):
    """
    Watches a HDFS directory, and generates file names on the output, one for each file that is found in the directory.

    Example, scanning for files in testDir directory::

        import streamsx.hdfs as hdfs
        from streamsx.topology.topology import Topology

        dir = '/user/streamsadmin/testDir'
        config = {
            'initDelay': 2.0,
            'sleepTime' : 2.0,
            'pattern' : 'sample.*txt'
        }       

        s = topo.source(hdfs.DirectoryScan(directory=dir, **config))

    Example, scanning for files with "csv" file extension::

        s = topo.source(hdfs.HdfsDirectoryScan(directory='/user/streamsadmin/testDir', pattern='.*\.csv$'))

    Attributes
    ----------
    credentials : dict|str
        The credentials of Hadoop cluster as dict or JSON string that contains the hdfs credentials key/value pairs for user, password and webhdfs .
    directory : str|Expression
        Specifies the name of the directory to be scanned
    pattern : str
        Instructs the operator to ignore file names that do not match the regular expression pattern
    schema : StreamSchema
        Output schema, defaults to CommonSchema.String
    options : kwargs
        The additional optional parameters as variable keyword arguments.
    """


    def __init__(self, credentials, directory, pattern=None, initDelay=None, schema=CommonSchema.String, **options):
        self.appConfigName = None
        self.localCredentials = credentials
        self.credentials = None
        self.configPath = None
        self.directory = directory
        self.schema = schema
        self.pattern = pattern
        self.sleepTime = None
        self.initDelay = initDelay
        self.authKeytab = None
        self.authPrincipal = None
        self.credFile = None
        self.hdfsPassword = None
        self.hdfsUri = None
        self.hdfsUser = None
        self.keyStorePassword = None
        self.keyStorePath = None
        self.libPath = None
        self.policyFilePath = None
        self.reconnectionBound = None
        self.reconnectionInterval = None
        self.reconnectionPolicy = None
        self.strictMode = None
        self.vmArg = None
  

        if 'appConfigName' in options:
            self.appConfigName = options.get('appConfigName')
        if 'hdfsUser' in options:
            self.hdfsUser = options.get('hdfsUser')
        if 'hdfsUri' in options:
            self.hdfsUri = options.get('hdfsUri')
        if 'hdfsPassword' in options:
            self.hdfsPassword = options.get('hdfsPassword')
        if 'sleepTime' in options:
            self.sleepTime = options.get('sleepTime')
        if 'initDelay' in options:
            self.initDelay = options.get('initDelay')
        if 'authKeytab' in options:
            self.authKeytab = options.get('authKeytab')
        if 'authPrincipal' in options:
            self.authPrincipal = options.get('authPrincipal')
        if 'configPath' in options:
            self.configPath = options.get('configPath')
        if 'strictMode' in options:
            self.strictMode = options.get('strictMode')  
        if 'keyStorePassword' in options:
            self.keyStorePassword = options.get('keyStorePassword')
        if 'keyStorePath' in options:
            self.keyStorePath = options.get('keyStorePath')
  

    @property
    def appConfigName(self):
        """
            str: The optional parameter appConfigName specifies the name of the application configuration that contains HDFS connection related configuration parameter credentials..
        """
        return self._appConfigName

    @appConfigName.setter
    def appConfigName(self, value):
        self._appConfigName = value

    @property
    def authKeytab(self):
        """
            str: The optional parameter authKeytab specifies the file that contains the encrypted keys for the user that is specified by the authPrincipal parameter. The operator uses this keytab file to authenticate the user. The keytab file is generated by the administrator. You must specify this parameter to use Kerberos authentication.
        """
        return self._authKeytab

    @authKeytab.setter
    def authKeytab(self, value):
        self._authKeytab = value

    @property
    def authPrincipal(self):
        """
            str: The optional parameter authPrincipal specifies the Kerberos principal that you use for authentication. This value is set to the principal that is created for the IBM Streams instance owner. You must specify this parameter if you want to use Kerberos authentication. 
        """
        return self._authPrincipal

    @authPrincipal.setter
    def authPrincipal(self, value):
        self._authPrincipal = value
     

    @property
    def configPath(self):
        """
            str: The optional parameter configPath specifies the path to the directory that contains the HDFS configuration file core-site.xml .
        """
        return self._configPath

    @configPath.setter
    def configPath(self, value):
        self._configPath = value

    @property
    def credFile(self):
        """
            str: The optional parameter credFile specifies a file that contains login credentials. The credentials are used to connect to WEBHDF remotely by using the schema: webhdfs://hdfshost:webhdfsport The credentials file must be a valid JSON string and must contain the hdfs credentials key/value pairs for user, password and webhdfs in JSON format. 
        """
        return self._credFile

    @credFile.setter
    def credFile(self, value):
        self._credFile = value

    @property
    def credentials(self):
        """
            str: The optional parameter credentials specifies the JSON string that contains the hdfs credentials key/value pairs for user, password and webhdfs .
        """
        return self._credentials

    @credentials.setter
    def credentials(self, value):
        self._credentials = value

    @property
    def hdfsPassword(self):
        """
            str: The parameter hdfsPassword specifies the password to use when you connecting to a Hadoop instance via WEBHDFS.
        """
        return self._hdfsPassword

    @hdfsPassword.setter
    def hdfsPassword(self, value):
        self._hdfsPassword = value

    @property
    def hdfsUri(self):
        """
            str: The parameter hdfsUri specifies the uniform resource identifier (URI) that you can use to connect to the HDFS file system. 
        """
        return self._hdfsUri

    @hdfsUri.setter
    def hdfsUri(self, value):
        self._hdfsUri = value


    @property
    def hdfsUser(self):
        """
            str: The parameter hdfsUser specifies the user ID to use when you connect to the HDFS file system. If this parameter is not specified, the operator uses the instance owner ID to connect to HDFS. .
        """
        return self._hdfsUser

    @hdfsUser.setter
    def hdfsUser(self, value):
        self._hdfsUser = value


    @property
    def initDelay(self):
        """
            float: The parameter initDelay specifies the time to wait in seconds before the operator HDFS2DirectoryScan reads the first file. The default value is 0 . 
        """
        return self._initDelay

    @initDelay.setter
    def initDelay(self, value):
        self._initDelay = value


    @property
    def keyStorePassword(self):
        """
            str: The optional parameter keyStorePassword is only supported when connecting to a WEBHDFS. It specifies the password for the keystore file. 
        """
        return self._keyStorePassword

    @keyStorePassword.setter
    def keyStorePassword(self, value):
        self._keyStorePassword = value

    @property
    def keyStorePath(self):
        """
            str: The optional parameter keyStorePath is only supported when connecting to a WEBHDFS. It specifies the path to the keystore file, which is in PEM format. The keystore file is used when making a secure connection to the HDFS server and must contain the public certificate of the HDFS server that will be connected to. 
        """
        return self._keyStorePath
    
    @keyStorePath.setter
    def keyStorePath(self, value):
        self._keyStorePath = value


    @property
    def libPath(self):
        """
            str: The optional parameter libPath specifies the absolute path to the directory that contains the Hadoop library files.
        """
        return self._libPath

  
    @libPath.setter
    def libPath(self, value):
        self._libPath = value

    @property
    def pattern(self):
        """
            str: The optional parameter pattern limits the file names that are listed to the names that match the specified regular expression. The HDFS2DirectoryScan operator ignores file names that do not match the specified regular expression. 
        """
        return self._pattern


    @pattern.setter
    def pattern(self, value):
        self._pattern = value

    @property
    def policyFilePath(self):
        """
            str: The optional parameter policyFilePath is relevant when connecting to IBM Analytics Engine on IBM Cloud. It specifies the path to the directory that contains the Java Cryptography Extension policy files (US_export_policy.jar and local_policy.jar).
        """
        return self._policyFilePath


    @policyFilePath.setter
    def policyFilePath(self, value):
        self._policyFilePath = value

    @property
    def reconnectionBound(self):
        """
            int: The optional parameter reconnectionBound specifies the number of successive connection attempts that occur when a connection fails or a disconnect occurs. It is used only when the reconnectionPolicy parameter is set to BoundedRetry; otherwise, it is ignored. The default value is 5 .
        """
        return self._reconnectionBound

    @reconnectionBound.setter
    def reconnectionBound(self, value):
        self._reconnectionBound = value



    @property
    def reconnectionInterval(self):
        """
            int: The optional parameter reconnectionInterval specifies the amount of time (in seconds) that the operator waits between successive connection attempts. It is used only when the reconnectionPolicy parameter is set to BoundedRetry or InfiniteRetry; othewise, it is ignored. The default value is 10 . 
        """
        return self._reconnectionInterval

    @reconnectionInterval.setter
    def reconnectionInterval(self, value):
        self._reconnectionInterval = value


    @property
    def reconnectionPolicy(self):
        """
            str: The optional parameter reconnectionPolicy specifies the policy that is used by the operator to handle HDFS connection failures. The valid values are: NoRetry, InfiniteRetry , and BoundedRetry . The default value is BoundedRetry .
        """
        return self._reconnectionPolicy

    @reconnectionPolicy.setter
    def reconnectionPolicy(self, value):
        self._reconnectionPolicy = value


    @property
    def sleepTime(self):
        """
            float: The parameter sleepTime specifies the minimum time between directory scans. The default value is 5.0 seconds.  . 
        """
        return self._sleepTime

    @sleepTime.setter
    def sleepTime(self, value):
        self._sleepTime = value


    @property
    def strictMode(self):
        """
            bool: The parameter sleepTime specifies the minimum time between directory scans. The default value is 5.0 seconds.
        """
        return self._strictMode

    @strictMode.setter
    def strictMode(self, value):
        self._strictMode = value

    @property
    def vmArg(self):
        """
            str: The optional parameter vmArg parameter to specify additional JVM arguments that are required by the specific invocation of the operator. 
        """
        return self._vmArg

    @vmArg.setter
    def vmArg(self, value):
        self._vmArg = value


    def populate(self, topology, name, **options):

        self.credentials, self.hdfsUri, self.hdfsUser, self.hdfsPassword, self.configPath=_setCredentials(self.localCredentials, topology)
  
        if self.sleepTime is not None:
            self.sleepTime = streamsx.spl.types.float64(self.sleepTime)
        if self.initDelay is not None:
            self.initDelay = streamsx.spl.types.float64(self.initDelay)

        if self.strictMode is not None:
            if self.strictMode is True:
                self.strictMode = streamsx.spl.op.Expression.expression('true')
            else:
                self.strictMode = streamsx.spl.op.Expression.expression('false')


            
        _op = _HDFS2DirectoryScan(topology=topology, \
                        schema=self.schema, \
                        appConfigName=self.appConfigName, \
                        authKeytab=self.authKeytab, \
                        authPrincipal=self.authPrincipal, \
                        configPath=self.configPath, \
                        credFile=self.credFile, \
                        credentials=self.credentials, \
                        directory=self.directory, \
                        hdfsPassword=self.hdfsPassword, \
                        hdfsUri=self.hdfsUri, \
                        hdfsUser=self.hdfsUser, \
                        initDelay=self.initDelay, \
                        keyStorePassword=self.keyStorePassword, \
                        keyStorePath=self.keyStorePath, \
                        libPath=self.libPath, \
                        pattern=self.pattern, \
                        policyFilePath=self.policyFilePath, \
                        reconnectionBound=self.reconnectionBound, \
                        reconnectionInterval=self.reconnectionInterval, \
                        reconnectionPolicy=self.reconnectionPolicy, \
                        sleepTime=self.sleepTime, \
                        strictMode=self.strictMode, \
                        vmArg=self.vmArg, \
                        name=name)

        return _op.stream


class HdfsFileSink(streamsx.topology.composite.ForEach):
    """
    Write a stream to a file

    .. note:: Only the last component of the path name is created if it does not exist. All directories in the path name up to the last component must exist.

    Example for writing a stream to a file::

        import streamsx.hdfs as hdfs
        from streamsx.topology.topology import Topology

        topo = Topology()
        s = topo.source(['Hello', 'World!']).as_string()
        s.for_each(hdfs.HdfsFileSink(credentials=credentials, file='/user/hdfs/data.txt'))

    Example with specifying parameters as kwargs and construct the name of the file with the attribute ``filename`` of the input stream::

        config = {
            'hdfsUser': 'hdfs',
            'tuplesPerFile': 50000
        }
        fsink = hdfs.HdfsFileSink(file=streamsx.spl.op.Expression.expression('pytest1/sample4%FILENUM.txt''), **config)
        to_file.for_each(fsink)

    Attributes
    ----------
    credentials : dict|str
        The credentials of Hadoop cluster as dict or JSON string that contains the hdfs credentials key/value pairs for user, password and webhdfs .
    file : str
        Name of the output file.
    options : kwargs
        The additional optional parameters as variable keyword arguments.
    """


    def __init__(self, credentials, file, **options):
        self.file = file
        self.localCredentials = credentials
        self.appConfigName = None
        self.authKeytab = None
        self.authPrincipal = None
        self.bytesPerFile = None
        self.closeOnPunct = None
        self.configPath = None
        self.credFile = None
        self.credentials = None
        self.encoding = None
        self.fileAttributeName = None
        self.schema = None
        self.hdfsPassword = None
        self.hdfsUri = None
        self.hdfsUser = None
        self.keyStorePassword = None
        self.keyStorePath = None
        self.libPath = None
        self.policyFilePath = None
        self.reconnectionBound = None
        self.reconnectionInterval = None
        self.reconnectionPolicy = None
        self.tempFile = None
        self.timeFormat = None
        self.timePerFile = None
        self.tuplesPerFile = None
        self.vmArg = None
        
        
        if 'appConfigName' in options:
            self.appConfigName = options.get('appConfigName')
        if 'bytesPerFile' in options:
            self.bytesPerFile = options.get('bytesPerFile')
        if 'closeOnPunct' in options:
            self.closeOnPunct = options.get('closeOnPunct')
        if 'hdfsUser' in options:
            self.hdfsUser = options.get('hdfsUser')
        if 'hdfsUri' in options:
            self.hdfsUri = options.get('hdfsUri')
        if 'hdfsPassword' in options:
            self.hdfsPassword = options.get('hdfsPassword')
        if 'sleepTime' in options:
            self.sleepTime = options.get('sleepTime')
        if 'initDelay' in options:
            self.initDelay = options.get('initDelay')
        if 'authKeytab' in options:
            self.authKeytab = options.get('authKeytab')
        if 'authPrincipal' in options:
            self.authPrincipal = options.get('authPrincipal')
        if 'configPath' in options:
            self.configPath = options.get('configPath')
        if 'timeFormat' in options:
            self.timeFormat = options.get('timeFormat')
        if 'timePerFile' in options:
            self.timePerFile = options.get('timePerFile')
        if 'tuplesPerFile' in options:
            self.tuplesPerFile = options.get('tuplesPerFile')
        if 'keyStorePassword' in options:
            self.keyStorePassword = options.get('keyStorePassword')
        if 'keyStorePath' in options:
            self.keyStorePath = options.get('keyStorePath')
 
    @property
    def appConfigName(self):
        """
            str: The optional parameter appConfigName specifies the name of the application configuration that contains HDFS connection related configuration parameter credentials..
        """
        return self._appConfigName

    @appConfigName.setter
    def appConfigName(self, value):
        self._appConfigName = value

    @property
    def authKeytab(self):
        """
            str: The optional parameter authKeytab specifies the file that contains the encrypted keys for the user that is specified by the authPrincipal parameter. The operator uses this keytab file to authenticate the user. The keytab file is generated by the administrator. You must specify this parameter to use Kerberos authentication.
        """
        return self._authKeytab

    @authKeytab.setter
    def authKeytab(self, value):
        self._authKeytab = value

    @property
    def authPrincipal(self):
        """
            str: The optional parameter authPrincipal specifies the Kerberos principal that you use for authentication. This value is set to the principal that is created for the IBM Streams instance owner. You must specify this parameter if you want to use Kerberos authentication. 
        """
        return self._authPrincipal

    @authPrincipal.setter
    def authPrincipal(self, value):
        self._authPrincipal = value
     

    @property
    def bytesPerFile(self):
        """
            int: This parameter specifies the approximate size of the output file, in bytes. When the file size exceeds the specified number of bytes, the current output file is closed and a new file is opened. The bytesPerFile, timePerFile, and tuplesPerFile parameters are mutually exclusive; you can specify only one of these parameters at a time.
        """
        return self._bytesPerFile

    @bytesPerFile.setter
    def bytesPerFile(self, value):
        self._bytesPerFile = value



    @property
    def configPath(self):
        """
            str: The optional parameter configPath specifies the path to the directory that contains the HDFS configuration file core-site.xml .
        """
        return self._configPath

    @configPath.setter
    def configPath(self, value):
        self._configPath = value

    @property
    def credFile(self):
        """
            str: The optional parameter credFile specifies a file that contains login credentials. The credentials are used to connect to WEBHDF remotely by using the schema: webhdfs://hdfshost:webhdfsport The credentials file must be a valid JSON string and must contain the hdfs credentials key/value pairs for user, password and webhdfs in JSON format. 
        """
        return self._credFile

    @credFile.setter
    def credFile(self, value):
        self._credFile = value

    @property
    def credentials(self):
        """
            str: The optional parameter credentials specifies the JSON string that contains the hdfs credentials key/value pairs for user, password and webhdfs .
        """
        return self._credentials

    @credentials.setter
    def credentials(self, value):
        self._credentials = value

    @property
    def closeOnPunct(self):
        """
            bool: This parameter specifies whether the operator closes the current output file and creates a new file when a punctuation marker is received. The default value is false . 
        """
        return self._closeOnPunct

    @closeOnPunct.setter
    def closeOnPunct(self, value):
        self._closeOnPunct = value

    @property
    def fileAttributeName(self):
        """
            str: If set, this points to an attribute containing the filename. The operator will close a file when value of this attribute changes. If the string contains substitutions, the check for a change happens before substituations, and the filename contains the substitutions based on the first tuple. 
        """
        return self._fileAttributeName

    @fileAttributeName.setter
    def fileAttributeName(self, value):
        self._fileAttributeName = value

    @property
    def hdfsPassword(self):
        """
            str: The parameter hdfsPassword specifies the password to use when you connecting to a Hadoop instance via WEBHDFS.
        """
        return self._hdfsPassword

    @hdfsPassword.setter
    def hdfsPassword(self, value):
        self._hdfsPassword = value

    @property
    def hdfsUri(self):
        """
            str: The parameter hdfsUri specifies the uniform resource identifier (URI) that you can use to connect to the HDFS file system. 
        """
        return self._hdfsUri

    @hdfsUri.setter
    def hdfsUri(self, value):
        self._hdfsUri = value


    @property
    def hdfsUser(self):
        """
            str: The parameter hdfsUser specifies the user ID to use when you connect to the HDFS file system. If this parameter is not specified, the operator uses the instance owner ID to connect to HDFS. .
        """
        return self._hdfsUser

    @hdfsUser.setter
    def hdfsUser(self, value):
        self._hdfsUser = value


    @property
    def initDelay(self):
        """
            float: The parameter initDelay specifies the time to wait in seconds before the operator HDFS2DirectoryScan reads the first file. The default value is 0 . 
        """
        return self._initDelay

    @initDelay.setter
    def initDelay(self, value):
        self._initDelay = value


    @property
    def keyStorePassword(self):
        """
            str: The optional parameter keyStorePassword is only supported when connecting to a WEBHDFS. It specifies the password for the keystore file. 
        """
        return self._keyStorePassword

    @keyStorePassword.setter
    def keyStorePassword(self, value):
        self._keyStorePassword = value

    @property
    def keyStorePath(self):
        """
            str: The optional parameter keyStorePath is only supported when connecting to a WEBHDFS. It specifies the path to the keystore file, which is in PEM format. The keystore file is used when making a secure connection to the HDFS server and must contain the public certificate of the HDFS server that will be connected to. 
        """
        return self._keyStorePath
    
    @keyStorePath.setter
    def keyStorePath(self, value):
        self._keyStorePath = value


    @property
    def libPath(self):
        """
            str: The optional parameter libPath specifies the absolute path to the directory that contains the Hadoop library files.
        """
        return self._libPath

  
    @libPath.setter
    def libPath(self, value):
        self._libPath = value

    @property
    def policyFilePath(self):
        """
            str: The optional parameter policyFilePath is relevant when connecting to IBM Analytics Engine on IBM Cloud. It specifies the path to the directory that contains the Java Cryptography Extension policy files (US_export_policy.jar and local_policy.jar).
        """
        return self._policyFilePath


    @policyFilePath.setter
    def policyFilePath(self, value):
        self._policyFilePath = value

    @property
    def reconnectionBound(self):
        """
            int: The optional parameter reconnectionBound specifies the number of successive connection attempts that occur when a connection fails or a disconnect occurs. It is used only when the reconnectionPolicy parameter is set to BoundedRetry; otherwise, it is ignored. The default value is 5 .
        """
        return self._reconnectionBound

    @reconnectionBound.setter
    def reconnectionBound(self, value):
        self._reconnectionBound = value


    @property
    def reconnectionInterval(self):
        """
            int: The optional parameter reconnectionInterval specifies the amount of time (in seconds) that the operator waits between successive connection attempts. It is used only when the reconnectionPolicy parameter is set to BoundedRetry or InfiniteRetry; othewise, it is ignored. The default value is 10 . 
        """
        return self._reconnectionInterval

    @reconnectionInterval.setter
    def reconnectionInterval(self, value):
        self._reconnectionInterval = value


    @property
    def reconnectionPolicy(self):
        """
            str: The optional parameter reconnectionPolicy specifies the policy that is used by the operator to handle HDFS connection failures. The valid values are: NoRetry, InfiniteRetry , and BoundedRetry . The default value is BoundedRetry .
        """
        return self._reconnectionPolicy

    @reconnectionPolicy.setter
    def reconnectionPolicy(self, value):
        self._reconnectionPolicy = value


    @property
    def sleepTime(self):
        """
            float: The parameter sleepTime specifies the minimum time between directory scans. The default value is 5.0 seconds.  . 
        """
        return self._sleepTime

    @sleepTime.setter
    def sleepTime(self, value):
        self._sleepTime = value


    @property
    def tempFile(self):
        """
            str: This parameter specifies the name of the file that the operator writes to. When the file is closed the file is renamed to the final filename defined by the file parameter or fileAttributeName parameter. 
        """
        return self._tempFile

    @tempFile.setter
    def tempFile(self, value):
        self._tempFile = value


    @property
    def timeFormat(self):
        """
            str: This parameter specifies the time format to use when the file parameter value contains %TIME . The parameter value must contain conversion specifications that are supported by the java.text.SimpleDateFormat. The default format is yyyyMMdd_HHmmss . 
        """
        return self._timeFormat

    @timeFormat.setter
    def timeFormat(self, value):
        self._timeFormat = value


    @property
    def timePerFile(self):
        """
            float: This parameter specifies the approximate time, in seconds, after which the current output file is closed and a new file is opened for writing. The bytesPerFile, timePerFile, and tuplesPerFile parameters are mutually exclusive; you can specify only one of these parameters. 
        """
        return self._timePerFile

    @timePerFile.setter
    def timePerFile(self, value):
        self.timePerFile = value

    @property
    def tuplesPerFile(self):
        """
            int: This parameter specifies the maximum number of tuples that can be received for each output file. When the specified number of tuples are received, the current output file is closed and a new file is opened for writing. The bytesPerFile, timePerFile, and tuplesPerFile parameters are mutually exclusive; you can specify only one of these parameters at a time.
        """
        return self._tuplesPerFile

    @tuplesPerFile.setter
    def tuplesPerFile(self, value):
        self._tuplesPerFile = value

    @property
    def vmArg(self):
        """
            str: The optional parameter vmArg parameter to specify additional JVM arguments that are required by the specific invocation of the operator. 
        """
        return self._vmArg

    @vmArg.setter
    def vmArg(self, value):
        self._vmArg = value

  


    @property
    def bytesPerFile(self):
        """
            int: Specifies the approximate size of the output file, in bytes. When the file size exceeds the specified number of bytes, the current output file is closed and a new file is opened. This parameter must be specified when the :py:meth:`~streamsx.standard.files.FileSink.close_mode` parameter is set to size.
        """
        return self._bytesPerFile

    @bytesPerFile.setter
    def bytesPerFile(self, value):
        self._bytesPerFile = value


    @property
    def encoding(self):
        """
             str: Specifies the character set encoding that is used in the output file. Data that is written to the output file is converted from the UTF-8 character set to the specified character set before any compression is performed. The encoding parameter is not valid with formats bin or block.
        """
        return self._encoding

    @encoding.setter
    def encoding(self, value):
        self._encoding = value



    @property
    def timePerFile(self):
        """
            float: Specifies the approximate time, in seconds, after which the current output file is closed and a new file is opened. If the :py:meth:`~streamsx.standard.files.FileSink.close_mode` parameter is set to time, this parameter must be specified.
        """
        return self._timePerFile

    @timePerFile.setter
    def timePerFile(self, value):
        self._timePerFile = value


    def populate(self, topology, stream, name, **options) -> streamsx.topology.topology.Sink:

    
        self.credentials, self.hdfsUri, self.hdfsUser, self.hdfsPassword, self.configPath=_setCredentials(self.localCredentials, topology)
       
        if self.bytesPerFile is not None:
            self.bytesPerFile = streamsx.spl.types.int64(self.bytesPerFile)
        if self.timePerFile is not None:
            self.timePerFile = streamsx.spl.types.float64(self.timePerFile)
        if self.tuplesPerFile is not None:
            self.tuplesPerFile = streamsx.spl.types.int64(self.tuplesPerFile)

        if self.closeOnPunct is not None:
            if self.closeOnPunct is True:
                self.closeOnPunct = streamsx.spl.op.Expression.expression('true')
            else:
                self.closeOnPunct = streamsx.spl.op.Expression.expression('false')


        _op = _HDFS2FileSink(stream=stream, \
                        appConfigName=self.appConfigName, \
                        authKeytab=self.authKeytab, \
                        authPrincipal=self.authPrincipal, \
                        bytesPerFile=self.bytesPerFile, \
                        closeOnPunct=self.closeOnPunct, \
                        configPath=self.configPath, \
                        credFile=self.credFile, \
                        credentials=self.credentials, \
                        encoding=self.encoding, \
                        file=self.file, \
                        fileAttributeName=self.fileAttributeName, \
                        hdfsPassword=self.hdfsPassword, \
                        hdfsUri=self.hdfsUri, \
                        hdfsUser=self.hdfsUser, \
                        keyStorePassword=self.keyStorePassword, \
                        keyStorePath=self.keyStorePath, \
                        libPath=self.libPath, \
                        policyFilePath=self.policyFilePath, \
                        reconnectionBound=self.reconnectionBound, \
                        reconnectionInterval=self.reconnectionInterval, \
                        reconnectionPolicy=self.reconnectionPolicy, \
                        tempFile=self.tempFile, \
                        timeFormat=self.timeFormat, \
                        timePerFile=self.timePerFile, \
                        tuplesPerFile=self.tuplesPerFile, \
                        vmArg=self.vmArg, \
                        name=name)

        return streamsx.topology.topology.Sink(_op)

class HdfsFileSource(streamsx.topology.composite.Map):
    """
    Reads HDFS files given by input stream and generates tuples with the file content on the output stream.


    Example, scanning for HDFS files in pytest directory and reading files via HdfsFileSource::

        import streamsx.standard.files as files
        import streamsx.hdfs as hdfs
        sample_schema = StreamSchema('tuple<rstring directory>')
        options = {
            'initDelay': 2.0,
            'sleepTime' : 2.0,
            'pattern' : 'sample.*txt'
        }       

        scannedHdfsFiles = topo.source(hdfs.HdfsDirectoryScan(credentials=credentials, directory='pytest', schema=sample_schema, **options))

        sourceParamaters = {
            'configPath' : hdfs_cfg_file
        }

        source_schema = StreamSchema('tuple<rstring line>')

        fsource = scannedHdfsFiles.map(hdfs.HdfsFileSource(credentials=hdfs_cfg_file, schema=source_schema, **sourceParamaters))


    Attributes
    ----------
    credentials : dict|str
        The credentials of Hadoop cluster as dict or JSON string that contains the hdfs credentials key/value pairs for user, password and webhdfs .
    schema : StreamSchema
        Output schema, defaults to CommonSchema.String
    options : kwargs
        The additional optional parameters as variable keyword arguments.
    """


    def __init__(self, credentials, schema=CommonSchema.String, **options):
        self.appConfigName = None
        self.authKeytab = None
        self.authPrincipal = None
        self.blockSize = None        
        self.encoding = None        
        self.localCredentials = credentials
        self.credentials = None
        self.configPath = None
        self.file = None
        self.schema = schema
        self.credFile = None
        self.hdfsPassword = None
        self.hdfsUri = None
        self.hdfsUser = None
        self.initDelay = None
        self.keyStorePassword = None
        self.keyStorePath = None
        self.libPath = None
        self.policyFilePath = None
        self.reconnectionBound = None
        self.reconnectionInterval = None
        self.reconnectionPolicy = None
        self.vmArg = None
  

        if 'appConfigName' in options:
            self.appConfigName = options.get('appConfigName')
        if 'blockSize' in options:
            self.configPath = options.get('blockSize')
        if 'hdfsUser' in options:
            self.hdfsUser = options.get('hdfsUser')
        if 'hdfsUri' in options:
            self.hdfsUri = options.get('hdfsUri')
        if 'hdfsPassword' in options:
            self.hdfsPassword = options.get('hdfsPassword')
        if 'initDelay' in options:
            self.initDelay = options.get('initDelay')
        if 'authKeytab' in options:
            self.authKeytab = options.get('authKeytab')
        if 'authPrincipal' in options:
            self.authPrincipal = options.get('authPrincipal')
        if 'configPath' in options:
            self.configPath = options.get('configPath')
        if 'keyStorePassword' in options:
            self.keyStorePassword = options.get('keyStorePassword')
        if 'keyStorePath' in options:
            self.keyStorePath = options.get('keyStorePath')
  


    @property
    def appConfigName(self):
        """
            str: The optional parameter appConfigName specifies the name of the application configuration that contains HDFS connection related configuration parameter credentials..
        """
        return self._appConfigName

    @appConfigName.setter
    def appConfigName(self, value):
        self._appConfigName = value

    @property
    def authKeytab(self):
        """
            str: The optional parameter authKeytab specifies the file that contains the encrypted keys for the user that is specified by the authPrincipal parameter. The operator uses this keytab file to authenticate the user. The keytab file is generated by the administrator. You must specify this parameter to use Kerberos authentication.
        """
        return self._authKeytab

    @authKeytab.setter
    def authKeytab(self, value):
        self._authKeytab = value

    @property
    def authPrincipal(self):
        """
            str: The optional parameter authPrincipal specifies the Kerberos principal that you use for authentication. This value is set to the principal that is created for the IBM Streams instance owner. You must specify this parameter if you want to use Kerberos authentication. 
        """
        return self._authPrincipal

    @authPrincipal.setter
    def authPrincipal(self, value):
        self._authPrincipal = value
    @property
    def blockSize(self):
        """
            int: The optional parameter blockSize specifies the maximum number of bytes to be read at one time when reading a file into binary mode (ie, into a blob); thus, it is the maximum size of the blobs on the output stream. The parameter is optional, and defaults to 4096 . 
        """
        return self._blockSize

    @blockSize.setter
    def blockSize(self, value):
        self._blockSize = value
     

    @property
    def configPath(self):
        """
            str: The optional parameter configPath specifies the path to the directory that contains the HDFS configuration file core-site.xml .
        """
        return self._configPath

    @configPath.setter
    def configPath(self, value):
        self._configPath = value

    @property
    def credFile(self):
        """
            str: The optional parameter credFile specifies a file that contains login credentials. The credentials are used to connect to WEBHDF remotely by using the schema: webhdfs://hdfshost:webhdfsport The credentials file must be a valid JSON string and must contain the hdfs credentials key/value pairs for user, password and webhdfs in JSON format. 
        """
        return self._credFile

    @credFile.setter
    def credFile(self, value):
        self._credFile = value

    @property
    def credentials(self):
        """
            str: The optional parameter credentials specifies the JSON string that contains the hdfs credentials key/value pairs for user, password and webhdfs .
        """
        return self._credentials

    @credentials.setter
    def credentials(self, value):
        self._credentials = value

    @property
    def encoding(self):
        """
            str: This optional parameter specifies the encoding to use when reading files. The default value is UTF-8 . 
        """
        return self._encoding

    @encoding.setter
    def encoding(self, value):
        self._encoding = value


    @property
    def file(self):
        """
            str: This parameter specifies the name of the file that the operator opens and reads. This parameter must be specified when the optional input port is not configured. If the optional input port is used and the file name is specified, the operator generates an error. 
        """
        return self._file

    @file.setter
    def file(self, value):
        self._file = value


    @property
    def hdfsPassword(self):
        """
            str: The parameter hdfsPassword specifies the password to use when you connecting to a Hadoop instance via WEBHDFS.
        """
        return self._hdfsPassword

    @hdfsPassword.setter
    def hdfsPassword(self, value):
        self._hdfsPassword = value

    @property
    def hdfsUri(self):
        """
            str: The parameter hdfsUri specifies the uniform resource identifier (URI) that you can use to connect to the HDFS file system. 
        """
        return self._hdfsUri

    @hdfsUri.setter
    def hdfsUri(self, value):
        self._hdfsUri = value


    @property
    def hdfsUser(self):
        """
            str: The parameter hdfsUser specifies the user ID to use when you connect to the HDFS file system. If this parameter is not specified, the operator uses the instance owner ID to connect to HDFS. .
        """
        return self._hdfsUser

    @hdfsUser.setter
    def hdfsUser(self, value):
        self._hdfsUser = value


    @property
    def initDelay(self):
        """
            float: The parameter initDelay specifies the time to wait in seconds before the operator HDFS2FileSource reads the first file. The default value is 0 . 
        """
        return self._initDelay

    @initDelay.setter
    def initDelay(self, value):
        self._initDelay = value


    @property
    def keyStorePassword(self):
        """
            str: The optional parameter keyStorePassword is only supported when connecting to a WEBHDFS. It specifies the password for the keystore file. 
        """
        return self._keyStorePassword

    @keyStorePassword.setter
    def keyStorePassword(self, value):
        self._keyStorePassword = value

    @property
    def keyStorePath(self):
        """
            str: The optional parameter keyStorePath is only supported when connecting to a WEBHDFS. It specifies the path to the keystore file, which is in PEM format. The keystore file is used when making a secure connection to the HDFS server and must contain the public certificate of the HDFS server that will be connected to. 
        """
        return self._keyStorePath
    
    @keyStorePath.setter
    def keyStorePath(self, value):
        self._keyStorePath = value


    @property
    def libPath(self):
        """
            str: The optional parameter libPath specifies the absolute path to the directory that contains the Hadoop library files.
        """
        return self._libPath

  
    @libPath.setter
    def libPath(self, value):
        self._libPath = value

    @property
    def policyFilePath(self):
        """
            str: The optional parameter policyFilePath is relevant when connecting to IBM Analytics Engine on IBM Cloud. It specifies the path to the directory that contains the Java Cryptography Extension policy files (US_export_policy.jar and local_policy.jar).
        """
        return self._policyFilePath


    @policyFilePath.setter
    def policyFilePath(self, value):
        self._policyFilePath = value

    @property
    def reconnectionBound(self):
        """
            int: The optional parameter reconnectionBound specifies the number of successive connection attempts that occur when a connection fails or a disconnect occurs. It is used only when the reconnectionPolicy parameter is set to BoundedRetry; otherwise, it is ignored. The default value is 5 .
        """
        return self._reconnectionBound

    @reconnectionBound.setter
    def reconnectionBound(self, value):
        self._reconnectionBound = value



    @property
    def reconnectionInterval(self):
        """
            int: The optional parameter reconnectionInterval specifies the amount of time (in seconds) that the operator waits between successive connection attempts. It is used only when the reconnectionPolicy parameter is set to BoundedRetry or InfiniteRetry; othewise, it is ignored. The default value is 10 . 
        """
        return self._reconnectionInterval

    @reconnectionInterval.setter
    def reconnectionInterval(self, value):
        self._reconnectionInterval = value


    @property
    def reconnectionPolicy(self):
        """
            str: The optional parameter reconnectionPolicy specifies the policy that is used by the operator to handle HDFS connection failures. The valid values are: NoRetry, InfiniteRetry , and BoundedRetry . The default value is BoundedRetry .
        """
        return self._reconnectionPolicy

    @reconnectionPolicy.setter
    def reconnectionPolicy(self, value):
        self._reconnectionPolicy = value


    @property
    def vmArg(self):
        """
            str: The optional parameter vmArg parameter to specify additional JVM arguments that are required by the specific invocation of the operator. 
        """
        return self._vmArg

    @vmArg.setter
    def vmArg(self, value):
        self._vmArg = value

    def populate(self, topology, stream, schema, name, **options):

        self.credentials, self.hdfsUri, self.hdfsUser, self.hdfsPassword, self.configPath=_setCredentials(self.localCredentials, topology)
  
        if self.initDelay is not None:
            self.initDelay = streamsx.spl.types.float64(self.initDelay)
        if self.blockSize is not None:
            self.blockSize = streamsx.spl.types.int32(self.blockSize)
        if self.authKeytab is not None:
            self.authKeytab = streamsx.spl.op.Expression.expression(self.authKeytab)
            
 

        _op = _HDFS2FileSource(stream=stream, \
                        schema=self.schema, \
                        appConfigName=self.appConfigName, \
                        authKeytab=self.authKeytab, \
                        authPrincipal=self.authPrincipal, \
                        blockSize=self.blockSize, \
                        encoding=self.encoding, \
                        configPath=self.configPath, \
                        credFile=self.credFile, \
                        credentials=self.credentials, \
                        file=self.file, \
                        hdfsPassword=self.hdfsPassword, \
                        hdfsUri=self.hdfsUri, \
                        hdfsUser=self.hdfsUser, \
                        initDelay=self.initDelay, \
                        keyStorePassword=self.keyStorePassword, \
                        keyStorePath=self.keyStorePath, \
                        libPath=self.libPath, \
                        policyFilePath=self.policyFilePath, \
                        reconnectionBound=self.reconnectionBound, \
                        reconnectionInterval=self.reconnectionInterval, \
                        reconnectionPolicy=self.reconnectionPolicy, \
                        vmArg=self.vmArg, \
                        name=name)

        return _op.outputs[0]
