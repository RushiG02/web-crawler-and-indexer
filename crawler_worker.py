import scrapy
from kafka import KafkaConsumer
import redis
import cassandra.cluster

consumer = KafkaConsumer('url_topic', bootstrap_servers='localhost:9092')
redis_client = redis.Redis(host='localhost', port=6379, db=0)
cluster = cassandra.cluster.Cluster(['localhost'])
session = cluster.connect('webcrawler')

class WebCrawler(scrapy.Spider):
    name = 'web_crawler'

    def parse(self, response):
        url = response.url
        content = response.text
        
        redis_client.set(url, content)
        
        session.execute("""
            INSERT INTO pages (url, content) VALUES (%s, %s)
        """, (url, content))

for message in consumer:
    scrapy.Request(url=message.value.decode('utf-8'), callback=WebCrawler.parse)
