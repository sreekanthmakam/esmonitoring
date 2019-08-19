ES Cluster Monitoring Python module
===================================

Background
-----------------

* python module to monitor the Elastic Search cluster
* Gathers all metrics and pushes to influx DB. From there Granfana will showcase in visual format

Pre - requisites
-----------------
- Install python2.7 and pip2.7

  - sudo su
  - yum install python27-python-pip
  - source scl_source enable python27
  - python2.7 --version
  - pip2.7 --version


- download git and clone the package

- Install required python packages

  - export http_proxy=http://10.243.127.53:80
  - export https_proxy=https://10.243.127.53:80
  - export no_proxy="0,1,2,3,4,5,6,7,8,9,100.100.173.59,yum-iad.oracle.com,127.0.0.1, localhost"
  - pip2.7 install -r requirements.txt



