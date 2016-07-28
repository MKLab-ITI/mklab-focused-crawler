mklab-focused-crawler
=====================

The main purpose of mklab-focused-crawler is fetching, parsing, analysis and indexing of web pages shared through social networks. Also this module collects multimedia content
embedded in webpages or shared in social media platforms and index it for future use.

The main pipeline of focused crawler is implemented as a storm topology, where the sequantial bolts perform a specific operation on the crawling procedure.



![focused crawler topology](https://github.com/MKLab-ITI/mklab-focused-crawler/blob/dice/imgs/storm%20topologies.png)
