mklab-focused-crawler
=====================

The main purpose of mklab-focused-crawler is fetching, parsing, analysis and indexing of web pages shared through social networks. Also this module collects multimedia content
embedded in webpages or shared in social media platforms and index it for future use.

The main pipeline of focused crawler is implemented as a storm topology, where the sequantial bolts perform a specific operation on the crawling procedure. The overall topology of focused crawler is depiced in the following figure.
![focused crawler topology](https://github.com/MKLab-ITI/mklab-focused-crawler/blob/dice/imgs/storm%20topologies.png)

The input stream consists of URLs: these refer either to arbitrary web pages or to social media pages. There are three spouts that are possible to inject URLs in the topology: a) one that periodically reads URLs from a running mongo database instance, b) one that listens to a Redis message broker following the Publish/Subscribe pattern, and c) one waiting for URLs from a Kafka queue.
The URLs fed to the crawler may be produced by any independent process. One possibility is to use the [Stream Manager project](https://github.com/MKLab-ITI/mklab-stream-manager).

```sh
  {
      "_id": "https://youtu.be/zvad7iztAIM",
      "url": "https://youtu.be/zvad7iztAIM",
      "date": ISODate("2016-07-11T10:37:17.0Z"),
      "reference": "Twitter#752451689530089473",
      "source": "Twitter"
  }
```

As URLs on Twitter are usually shortened, the first bolt in the pipeline([URLExpansionBolt](https://github.com/MKLab-ITI/mklab-focused-crawler/blob/dice/src/main/java/gr/iti/mklab/focused/crawler/bolts/webpages/URLExpansionBolt.java)) expands them to long form. The next bolt checks the type of the URLs and its crawling status. URLs that correspond to posts in popular social media platforms (e.g., https://www.youtube.com/watch?v=LHAZYK6x6iE) are redirected to a bolt named Social Media Retriever, which retrieves metadata from the respective platforms. URLs to
arbitrary web pages are emitted to a Fetcher and subsequently to a Parser bolt. Non-HTML content is discarded. The parsed content is then forwarded to the next bolt (Article Extraction) that attempts to extract articles and embedded media items. The extracted articles are indexed in a running Solr instance by the Text Indexer. The extracted media items, as well as the media items coming from the Social Media Retriever bolt are handled by the Media Text Indexer.
