import redis
from kafka import KafkaProducer

redis_client = redis.Redis(host='localhost', port=6379, db=0)
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def add_url_to_queue(url):
    if not redis_client.sismember("visited_urls", url):
        producer.send('url_topic', url.encode('utf-8'))
        redis_client.sadd("visited_urls", url)

# Add an initial URL
URLs = ['https://m.indiamart.com/proddetail/10kg-composite-cylinder-2851974348130.html',
'https://m.indiamart.com/proddetail/ling-mota-lamba-karne-ki-dava-ling-ka-size-badhane-ki-dawa-2849549849248.html',
'https://m.indiamart.com/proddetail/ling-mota-lamba-bada-khada-karne-wali-goli-dawa-capsule-medicine-tablet-2851951641497.html',
'https://m.indiamart.com/proddetail/captcha-typing-work-10435961991.html',
'https://m.indiamart.com/proddetail/300-tc-percale-cotton-fabric-2851184947562.html',
'https://m.indiamart.com/proddetail/3mm-5mm-sunboard-with-vinyl-print-22375154812.html',
'https://m.indiamart.com/proddetail/beretta-mod-bb-co2-pistol-21951154355.html',
'https://m.indiamart.com/proddetail/4-way-lycra-track-pant-26931585812.html',
'https://m.indiamart.com/proddetail/gta-v-grand-theft-auto-5-premium-edition-steam-25511521191.html',
'https://m.indiamart.com/proddetail/royal-enfield-classic-350-13520842530.html']
for url in URLs:
    add_url_to_queue("url")
