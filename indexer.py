import nltk
from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect('webcrawler')

def index_page(url, text):
    words = set(nltk.word_tokenize(text.lower()))
    for word in words:
        session.execute("""
            INSERT INTO index_table (word, url) VALUES (%s, %s)
        """, (word, url))

rows = session.execute("SELECT url, content FROM pages")
for row in rows:
    index_page(row.url, row.content)
