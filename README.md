mklab-focused-crawler
=====================

The main purpose of mklab-focused-crawler is fetching, parsing, analysis and indexing of web pages shared through social networks. Also this module collects multimedia content
embedded in webpages or shared in social media platforms and index it for future use.

The main pipeline of focused crawler is implemented as a storm topology, where the sequantial bolts perform a specific operation on the crawling procedure. The overall topology of focused crawler is depiced in the following figure.
![focused crawler topology](https://github.com/MKLab-ITI/mklab-focused-crawler/blob/dice/imgs/storm%20topologies.png)

The input stream consists of URLs: these refer either to arbitrary web pages or to social media pages. There are three spouts that are possible to inject URLs in the topology: a) one that periodically reads URLs from a running mongo database instance, b) one that listens to a Redis message broker following the Publish/Subscribe pattern, and c) one waiting for URLs from a Kafka queue.
The URLs fed to the crawler may be produced by any independent process. One possibility is to use the [Stream Manager project](https://github.com/MKLab-ITI/mklab-stream-manager).
