
import streamsx.hdfs as hdfs

from streamsx.topology.topology import Topology
import streamsx as streamsx
from streamsx.topology.tester import Tester
import streamsx.spl.toolkit as tk
import streamsx.rest as sr
import streamsx.spl.op as op
from streamsx.topology.schema import CommonSchema, StreamSchema


import unittest
import datetime
import os
import json

##
## Test assumptions
##
## Streaming analytics service or Streams instance running
## IBM cloud Analytics Engine service credentials are located in a file referenced by environment variable ANALYTICS_ENGINE.
## The core-site.xml is referenced by HDFS_SITE_XML environment variable.
## HDFS toolkit location is given by STREAMS_HDFS_TOOLKIT environment variable.
##
def toolkit_env_var():
    result = True
    try:
        os.environ['STREAMS_HDFS_TOOLKIT']
    except KeyError:
        result = False
    return result

def streams_install_env_var():
    result = True
    try:
        os.environ['STREAMS_INSTALL']
    except KeyError:
        result = False
    return result

def site_xml_env_var():
    result = True
    try:
        os.environ['HDFS_SITE_XML']
    except KeyError:
        result = False
    return result

def cloud_creds_env_var():
    result = True
    try:
        os.environ['ANALYTICS_ENGINE']
    except KeyError:
        result = False
    return result

class TestParams(unittest.TestCase):

    @unittest.skipIf(cloud_creds_env_var() == False, "Missing ANALYTICS_ENGINE environment variable.")
    def test_cloud_creds(self):
        creds_file = os.environ['ANALYTICS_ENGINE']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology()
        hdfs.scan(topo, credentials, 'a_dir')
        hdfs.scan(topo, credentials=credentials, directory='a_dir', init_delay=10.0)

    @unittest.skipIf(site_xml_env_var() == False, "Missing HDFS_SITE_XML environment variable.")
    def test_xml_creds(self):
        xml_file = os.environ['HDFS_SITE_XML']
        topo = Topology()
        hdfs.scan(topo, credentials=xml_file, directory='a_dir')
        hdfs.scan(topo, credentials=xml_file, directory='a_dir', pattern='*.txt', init_delay=datetime.timedelta(seconds=5))

    def test_bad_cred_param(self):
        topo = Topology()
        # expect ValueError because credentials is neither a dict nor a file
        self.assertRaises(TypeError, hdfs.scan, topo, credentials='invalid', directory='any_dir')
        # expect ValueError because credentials is not expected JSON format
        invalid_creds = json.loads('{"user" : "user", "password" : "xx", "uri" : "xxx"}')
        self.assertRaises(ValueError, hdfs.scan, topo, credentials=invalid_creds, directory='any_dir')

    @unittest.skipIf(cloud_creds_env_var() == False, "Missing ANALYTICS_ENGINE environment variable.")
    def test_bad_time_param(self):
        creds_file = os.environ['ANALYTICS_ENGINE']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology()
        # expect TypeError because init_delay is wrong type (string)
        self.assertRaises(TypeError, hdfs.scan, topo, credentials=credentials, directory='any_dir', init_delay='1')
        # expect ValueError because init_delay is too small (< 1 sec)
        self.assertRaises(ValueError, hdfs.scan, topo, credentials=credentials, directory='any_dir', init_delay=0.1)

    @unittest.skipIf(cloud_creds_env_var() == False, "Missing ANALYTICS_ENGINE environment variable.")
    def test_bad_close_file_param(self):
        creds_file = os.environ['ANALYTICS_ENGINE']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology()
        s = topo.source(['Hello World!']).as_string()
        # expect ValueError because bytesPerFile, timePerFile, and tuplesPerFile parameters are mutually exclusive.
        self.assertRaises(ValueError, hdfs.write, s, credentials=credentials, file='any_file', timePerFile=5, tuplesPerFile=5)
        self.assertRaises(ValueError, hdfs.write, s, credentials=credentials, file='any_file', bytesPerFile=5, timePerFile=5)
        self.assertRaises(ValueError, hdfs.write, s, credentials=credentials, file='any_file', bytesPerFile=5, tuplesPerFile=5)
        self.assertRaises(ValueError, hdfs.write, s, credentials=credentials, file='any_file', bytesPerFile=200, timePerFile=5, tuplesPerFile=5)



class TestFileSink(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print (str(self))

    def setUp(self):
        Tester.setup_distributed(self)
        self.hdfs_toolkit_location = os.environ['STREAMS_HDFS_TOOLKIT']


    def _build_only(self, name, topo):
        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)

     # ------------------------------------
    @unittest.skipIf(site_xml_env_var() == False, "HDFS_SITE_XML environment variable.")
    def test_HdfsFileSink(self):
        hdfs_cfg_file = os.environ['HDFS_SITE_XML']
        # credentials is the path to the HDSF *configuration file 'hdfs-site.xml'
        topo = Topology('test_composite')

        if self.hdfs_toolkit_location is not None:
            tk.add_toolkit(topo, self.hdfs_toolkit_location)

        
        pulse = op.Source(topo, "spl.utility::Beacon", 'tuple<rstring directory>', params = {'period':0.5, 'iterations':100})
        pulse.directory = pulse.output('"This line will be written into a HDFS file via HdfsFileSink. " + (rstring) IterationCount()')

        to_file = pulse.outputs[0]
        config = {
            'configPath' : hdfs_cfg_file
        }

        fsink = hdfs.HdfsFileSink(credentials=hdfs_cfg_file, file='pytest1/sample611.txt', **config)
        to_file.for_each(fsink)
        tester = Tester(topo)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)


class TestFileSource(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print (str(self))

    def setUp(self):
        Tester.setup_distributed(self)
        self.hdfs_toolkit_location = os.environ['STREAMS_HDFS_TOOLKIT']


    def _build_only(self, name, topo):
        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)

     # ------------------------------------
    @unittest.skipIf(site_xml_env_var() == False, "HDFS_SITE_XML environment variable.")
    def test_HdfsFileSource(self):
        hdfs_cfg_file = os.environ['HDFS_SITE_XML']
        # credentials is the path to the HDSF *configuration file 'hdfs-site.xml'
        topo = Topology('test_composite')

        if self.hdfs_toolkit_location is not None:
            tk.add_toolkit(topo, self.hdfs_toolkit_location)

        sample_schema = StreamSchema('tuple<rstring directory>')


        options = {
            'initDelay': 2.0,
            'sleepTime' : 2.0,
            'pattern' : 'sample.*txt'
        }       

        scanned = topo.source(hdfs.HdfsDirectoryScan(credentials=hdfs_cfg_file, directory='pytest1', schema=sample_schema, **options))

        scanned.print()
        
 
        sourceParamaters = {
            'configPath' : hdfs_cfg_file
        }

        source_schema = StreamSchema('tuple<rstring line>')

        fsource = scanned.map(hdfs.HdfsFileSource(credentials=hdfs_cfg_file, schema=source_schema, **sourceParamaters))

        fsource.print()
        tester = Tester(topo)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)




class TestDirScan(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print (str(self))

    def setUp(self):
        Tester.setup_distributed(self)
        self.hdfs_toolkit_location = os.environ['STREAMS_HDFS_TOOLKIT']


    def _build_only(self, name, topo):
        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)


     # ------------------------------------
    @unittest.skipIf(site_xml_env_var() == False, "HDFS_SITE_XML environment variable.")
    def test_HdfsDirectoryScan(self):
        hdfs_cfg_file = os.environ['HDFS_SITE_XML']
        # credentials is the path to the HDSF *configuration file 'hdfs-site.xml'
        topo = Topology('test_composite')

        if self.hdfs_toolkit_location is not None:
            tk.add_toolkit(topo, self.hdfs_toolkit_location)

        credentials=hdfs_cfg_file 
        directory='pytest1'
        print ('\n---------'+str(self))
        name = 'test_HdfsDirScan'
        topo = Topology(name)


        sample_schema = StreamSchema('tuple<rstring directory>')
       
        options = {
            'initDelay': 2.0,
            'sleepTime' : 2.0,
            'pattern' : 'sample.*txt'
        }       
        
        scannedFileNames = topo.source(hdfs.HdfsDirectoryScan(credentials, directory=directory, schema=sample_schema, **options))
        scannedFileNames.print(name='printScannedFileNames')
 
        tester = Tester(topo)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

 




class TestDistributed(unittest.TestCase):
    """ Test in local Streams instance with local toolkit from STREAMS_HDFS_TOOLKIT environment variable """

    @classmethod
    def setUpClass(self):
        print (str(self))

    def setUp(self):
        Tester.setup_distributed(self)
        self.hdfs_toolkit_location = os.environ['STREAMS_HDFS_TOOLKIT']


     # ------------------------------------
    @unittest.skipIf(site_xml_env_var() == False, "HDFS_SITE_XML environment variable.")
    def test_all_hdsf_operators(self):
        hdfs_cfg_file = os.environ['HDFS_SITE_XML']
        # credentials is the path to the HDSF *configuration file 'hdfs-site.xml'
        topo = Topology('test_all_hdsf_operators')

        if self.hdfs_toolkit_location is not None:
            tk.add_toolkit(topo, self.hdfs_toolkit_location)
        
        # creates an input stream
        fileSinkInputStream = topo.source(['This line will be written into a HDFS file.']).as_string()
 
        # writes a line into a HDFS file (HDFS2FileSink)
        fileSinkResults = hdfs.write(fileSinkInputStream, credentials=hdfs_cfg_file, file='pytest1/sample4%FILENUM.txt')
        fileSinkResults.print(name='printFileSinkResults')
 
        # scans an HDFS directory and return file names (HDFS2DirectoryScan)
        scannedFileNames = hdfs.scan(topo, credentials=hdfs_cfg_file, directory='pytest1', pattern='sample.*txt', init_delay=10)
        scannedFileNames.print(name='printScannedFileNames')

        # reads lines from a HDFS file (HDFS2FileSource)
        readLines = hdfs.read(scannedFileNames, credentials=hdfs_cfg_file)
        readLines.print(name='printReadLines')

        # copies files from HDFS into local disk "/tmp/" (HDFS2FileCopy)
        copyFileResults=hdfs.copy(scannedFileNames, credentials=hdfs_cfg_file, direction='copyToLocalFile' , hdfsFile=None,  hdfsFileAttrName='fileName', localFile='/tmp/')
        copyFileResults.print(name='printCopyFileResults')

        tester = Tester(topo)
        tester.tuple_count(readLines, 1, exact=False)
        # tester.run_for(80)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)


    # ------------------------------------
    @unittest.skipIf(cloud_creds_env_var() == False, "Missing ANALYTICS_ENGINE environment variable.")
    def test_hdfs_read_with_credentaials(self):
        ae_service_creds_file = os.environ['ANALYTICS_ENGINE']
        with open(ae_service_creds_file) as data_file:
            credentials = data_file.read()
        # credentials is as JSON string
        
        topo = Topology('test_hdfs_uri')

        if self.hdfs_toolkit_location is not None:
            tk.add_toolkit(topo, self.hdfs_toolkit_location)

        s = topo.source(['Hello World!']).as_string()
        result = hdfs.write(s, credentials=credentials, file='pytest/sample%FILENUM.txt')
        result.print()

        scanned_files = hdfs.scan(topo, credentials=credentials, directory='pytest', pattern='sample.*txt', init_delay=10)
        scanned_files.print()

        lines = hdfs.read(scanned_files, credentials=credentials)
        lines.print()

        tester = Tester(topo)
        tester.tuple_count(lines, 1, exact=True)
        #tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    # ------------------------------------


    # ------------------------------------
    @unittest.skipIf(cloud_creds_env_var() == False, "Missing ANALYTICS_ENGINE environment variable.")
    def test_hdfs_uri(self):
        ae_service_creds_file = os.environ['ANALYTICS_ENGINE']
        with open(ae_service_creds_file) as data_file:
            credentials = json.load(data_file)
        # credentials is dict
        topo = Topology('test_hdfs_uri')

        if self.hdfs_toolkit_location is not None:
            tk.add_toolkit(topo, self.hdfs_toolkit_location)
       # creates an input stream
        fileSinkInputStream = topo.source(['This line will be written into a HDFS file.']).as_string()
        result = hdfs.write(fileSinkInputStream, credentials=credentials, file='pytest/sample%FILENUM.txt')
        result.print()

        scanned_files = hdfs.scan(topo, credentials=credentials, directory='pytest', pattern='sample.*txt', init_delay=10)
        scanned_files.print()

        lines = hdfs.read(scanned_files, credentials=credentials)
        lines.print()

        tester = Tester(topo)
        tester.tuple_count(lines, 1, exact=True)
        #tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    # ------------------------------------


    @unittest.skipIf(cloud_creds_env_var() == False, "Missing ANALYTICS_ENGINE environment variable.")
    def test_close_on_tuples(self):
        ae_service_creds_file = os.environ['ANALYTICS_ENGINE']
        with open(ae_service_creds_file) as data_file:
            credentials = json.load(data_file)

        topo = Topology('test_hdfs_uri')

        if self.hdfs_toolkit_location is not None:
            tk.add_toolkit(topo, self.hdfs_toolkit_location)

        s = topo.source(['Hello World!','Hello','World','Hello World!','Hello','World']).as_string()
        result = hdfs.write(s, credentials=credentials, file='pytest/write_test%FILENUM.txt', tuplesPerFile=3)
        result.print()

        tester = Tester(topo)
        tester.tuple_count(result, 2, exact=True)
        #tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    # ------------------------------------


class TestCloud(TestDistributed):
    """ Test in Streaming Analytics Service using local toolkit from STREAMS_HDFS_TOOLKIT environment variable """

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()
        print(result +'\n')

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=False)
        self.hdfs_toolkit_location = os.environ['STREAMS_HDFS_TOOLKIT']


class TestCloudRemote(TestCloud):
    """ Test in Streaming Analytics Service using remote toolkit from cloud build service """

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.hdfs_toolkit_location = None


class TestICPRemote(TestDistributed):
    """ Test in Cloud Pak using remote toolkit from cloud build service """

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_distributed(self)
        self.hdfs_toolkit_location = None
