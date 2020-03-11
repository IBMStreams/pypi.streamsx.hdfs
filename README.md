# Python streamsx.hdfs package

This exposes SPL operators in the `com.ibm.streamsx.hdfs` toolkit as Python methods.

Package is organized using standard packaging to upload to PyPi.

The package is uploaded to PyPi in the standard way:
```
cd package
python setup.py sdist bdist_wheel upload -r pypi
```
Note: This is done using the `ibmstreams` account at pypi.org and requires `.pypirc` file containing the credentials in your home directory.

Package details: https://pypi.python.org/pypi/streamsx.hdfs

Documentation is using Sphinx and can be built locally using:
```
cd package/docs
make html
```

or

    ant doc

and viewed using
```
firefox package/docs/build/html/index.html
```

The documentation is also setup at `readthedocs.io`.

Documentation links:
* http://streamsxhdfs.readthedocs.io

## Version update

To change the version information of the Python package, edit following files:

- ./package/docs/source/conf.py
- ./package/streamsx/hdfs/\_\_init\_\_.py

When the development status changes, edit the *classifiers* in

- ./package/setup.py

When the documented sample must be changed, change it here:

- ./package/streamsx/hdfs/\_\_init\_\_.py
- ./package/DESC.txt

## Test

Package can be tested with TopologyTester using the [Streaming Analytics](https://www.ibm.com/cloud/streaming-analytics) service and [Analytics Engine](https://www.ibm.com/cloud/analytics-engine) service on IBM Cloud.

Analytics Engine service credentials are located in a file referenced by environment variable `ANALYTICS_ENGINE`.

Alternative the "core-site.xml" file can be specified for testing with the environment variable `HDFS_SITE_XML`.


### Test parameters only

This test does not require any Streams instance.

```
cd package
python3 -u -m unittest streamsx.hdfs.tests.test_hdfs.TestParams

```

### Test with local Streams instance

This test requires STREAMS_INSTALL set and a running Streams instance.

Required envionment variable for the `com.ibm.streamsx.hdfs` toolkit location: `STREAMS_HDFS_TOOLKIT`

```
cd package
python3 -u -m unittest streamsx.hdfs.tests.test_hdfs.TestDistributed
```

or

    ant test


### Composite Test with local Streams instance 

This test requires:
- The environment variable `STREAMS_INSTALL` set and the Streams instance is running.
- The environment variable `STREAMS_HDFS_TOOLKIT` set for the `com.ibm.streamsx.hdfs` toolkit location: 
- A runing Ibm HADOOP cluster with a running HDFS instance.
- The environment variable `HDFS_SITE_XML` set to the HDFS configuration file `core.site.xml`.

This test copies the HDFS configiration file 'core.site.xml' in the application directory `etc` and perform the following tests:

- The standard operator `Beacon` generates 1000 lines.
- The `HdfsFileSink` opeartor writes every 100 lines produced by Beacon operator in a new file in 'pytest' (sample41.txt, sample42.txt, ...)
- The `HdfsDirectoryScan` operator scans the directory `pytest` and delivers HDFS file names in output port.
- The `HdfsFileSource` operator reads HDFS files in directory `pytest` deliverd by HdfsDirectoryScan and returns the lines of files in output port.
- The `HdfsDirectoryScan` operator scans the directory `pytest` and delivers HDFS file names in output port.
- The `HdfsFileCopy` operator copies HDFS files from directory 'pytest' deliverd by HdfsDirectoryScan into local directory `/tmp/`


```
cd package
python3 -u -m unittest streamsx.hdfs.tests.test_hdfs.TestCompositeDistributed
```




### Test with Streaming Analytics Service

This requires Streaming Analytics service and IBM Analytics Engine service in IBM cloud.

Required envionment variable for the `com.ibm.streamsx.hdfs` toolkit  location: `STREAMS_HDFS_TOOLKIT`

```
cd package
python3 -u -m unittest streamsx.hdfs.tests.test_hdfs.TestCloud.test_close_on_tuples streamsx.hdfs.tests.test_hdfs.TestCloud.test_hdfs_uri
```

or

    ant test-sas


### Composite Test with IBM Analytic Engine

This test requires:
- The environment variable `STREAMS_INSTALL` set and the Streams instance is running.
- The environment variable `STREAMS_HDFS_TOOLKIT` set for the `com.ibm.streamsx.hdfs` toolkit location: 
- A runing Ibm Analytics Engine HADOOP cluster with a running HDFS instance.
- The environment variable `ANALYTICS_ENGINE` set to credential file that contains the Hadoop cluster webhdfs credentilas as a JSON string.

This test redas the IAE credentilas (HdfsUri, HdfsUser, HdfsPassword) from JSON file and perform the following tests:

- The standard operator `Beacon` generates 1000 lines.
- The `HdfsFileSink` opeartor writes every 100 lines produced by Beacon operator in a new file in 'pytest' (sample41.txt, sample42.txt, ...)
- The `HdfsDirectoryScan` operator scans the directory `pytest` and delivers HDFS file names in output port.
- The `HdfsFileSource` operator reads HDFS files in directory `pytest` deliverd by HdfsDirectoryScan and returns the lines of files in output port.


```
cd package
python3 -u -m unittest streamsx.hdfs.tests.test_hdfs.TestCompositeWebHdfs
```


#### Remote build

For using the toolkit from the build service (**force_remote_build**) run the test with:

Run the test with:

    ant test-sas-remote

or

```
cd package
python3 -u -m unittest streamsx.hdfs.tests.test_hdfs.TestCloudRemote.test_close_on_tuples streamsx.hdfs.tests.test_hdfs.TestCloudRemote.test_hdfs_uri
```

