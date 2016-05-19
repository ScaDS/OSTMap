# OSTMap

## Overview
OSTMap (Open Source Twitter Map)
OSTMap development started as one project at the [IT-Ringvorlesung 2016](https://www.informatik.uni-leipzig.de/ifi/kooperation/it-ringvorlesung/sommersemester-2016/). A team of six students with some help of two big data experts implements OSTMap over a period of 6 weeks.

OSTMap reads geotagged data from the twitter stream. We save it to an small hadoop cluster (1 master, 4 worker nodes) running HDP 2.4 with Apache Accumulo and Apache Flink. In addition we have a user interface to search for tweets by a term search and a map search. The results are presented as list or on the map. In addition we run some analysis batch jobs on this data.

## Screenshots


## Demo
Currently we don't run a public accessible instance. Is there someone who wants to sponsor one?

## Technologies
* we use Apache Flink as computation framework for stream and batch processing
* we use Apache Accumulo as storage
* Spring Boot for restservices
* AngularJS, Bootstrap and Leaflet for the frontend

## Modules
* stream_processing
 * the stream processing app - reads the twitter stream and writes the raw data, the index and calculated results to accumulo
* batch_processing
 * batch jobs over the complete dataset 
* commons
 * some common code used by other submodules
* accumulo_iterators
 * some custom iterators for querying accumulo
* rest_service
 * a spring boot application serving the ui and the rest services used by the ui

## Links
* for information regardning development see: [wiki](https://github.com/IIDP/OSTMap/wiki)
* for issues see: [issues](https://github.com/IIDP/OSTMap/issues)
* for our milestones see: [milestones](https://github.com/IIDP/OSTMap/milestones)

## Compiling
* to compile/shadowJar a subproject f.eg. execute "gradle shadowJar -p batch_processing"
* both, the stream process and the batch process needs to be a fat jar for flink -> gradle shadowJar
* the rest service is build with spring boot -> gradle build

## License
 Apache License Version 2.0, see LICENSE file
