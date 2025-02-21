import scrapy
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor, defer
from kafka import KafkaConsumer
import redis
import cassandra.cluster

# Kafka Consumer
consumer = KafkaConsumer(
    'url_topic',
    auto_offset_reset='earliest',
    bootstrap_servers='localhost:9092'
)

# Redis Client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Cassandra Connection
cluster = cassandra.cluster.Cluster(['localhost'])
session = cluster.connect()
session.set_keyspace('webcrawler')

class WebCrawler(scrapy.Spider):
    name = 'web_crawler'
    
    def __init__(self, urls, *args, **kwargs):
        super(WebCrawler, self).__init__(*args, **kwargs)
        self.start_urls = urls  # URLs to crawl

    def parse(self, response):
        url = response.url
        content = response.text

        print(f"Scraped: {url}")  # Debugging output

        # Store in Redis
        redis_client.set(url, content)

        # Store in Cassandra
        session.execute(
            "INSERT INTO pages (url, content) VALUES (%s, %s)", 
            (url, content)
        )

@defer.inlineCallbacks
def consume_and_crawl():
    runner = CrawlerRunner()  # Scrapy runner

    while True:
        urls = []
        print("Listening for messages...")

        for message in consumer:
            url = message.value.decode('utf-8')
            print(f"Received URL: {url}")
            urls.append(url)

            if len(urls) >= 3:  # Process in batches of 5 URLs
                yield runner.crawl(WebCrawler, urls=urls)
                urls = []

# Start the crawling process
consume_and_crawl()
reactor.run()  # Start Twisted event loop
