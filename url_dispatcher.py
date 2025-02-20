import redis
from kafka import KafkaProducer

redis_client = redis.Redis(host='localhost', port=6379, db=0)
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def add_url_to_queue(url):
    if not redis_client.sismember("visited_urls", url):
        producer.send('url_topic', url.encode('utf-8'))
        redis_client.sadd("visited_urls", url)

# Add an initial URL
URLs = ['https://m.indiamart.com/proddetail/10kg-composite-cylinder-2851974348130.html','https://www.supreme.co.in/cylinder/products/composite-gas-cylinder-24-5-l-10-kg-propane/o1','https://gascylinderfactory.en.made-in-china.com/product/MFgAQIxoqkRE/China-New-Arrival-10kg-LPG-Gas-Cylinder-Cooking-Composite-Gas-Cylinder.html',
'https://www.justdial.com/jdmart/Meerut/13-mm-Thick-Plain-Rectangular-Frameless-Designer-Glass-Door/pid-2228722238/9999PX121-X121-241218140802-Q2Y4','https://www.indiamart.com/proddetail/toughened-glass-door-22018521188.html?srsltid=AfmBOooswootSFp64lE_NNwbmyWod2p2UkGGv07-0CBUfrgH1gQPWYIU','https://materialdepot.in/rld-13-2057x838-laminate-doors-line-art-32-mm/product',
'https://dir.indiamart.com/impcat/crompton-motor/horsepower-15-hp-q13468154/','https://www.mjvaluemart.com/electric-motor-15-hp?srsltid=AfmBOoqbpiOjNSAjTehDJCMuruLd1gAP2d1xLfq6EMbji5BcayKh0Wcj','https://www.amazon.in/2880-pole-phase-mounted-motor/dp/B0CJFZ54QW',
'https://www.hondaindiapower.com/product-detail/eu70is','https://www.indiamart.com/proddetail/perkins-diesel-generator-manufacturer-goem-22330780797.html','https://www.amazon.in/AK4000-3000-Powered-Generator-Commercial-Warranty/dp/B0DVLJ1Q83?ref_=Oct_d_otopr_d_3638820031_0&pd_rd_w=MeLLA&content-id=amzn1.sym.25a1078a-aa23-4746-b8f2-37c5df5524ed&pf_rd_p=25a1078a-aa23-4746-b8f2-37c5df5524ed&pf_rd_r=EYH5ND57ZW5TWAA9CART&pd_rd_wg=ODzGj&pd_rd_r=3ca05b74-58a5-4a08-820b-5e6c92149f19&pd_rd_i=B0DVLJ1Q83']
for url in URLs:
    add_url_to_queue("url")
