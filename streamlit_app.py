import cassandra
from cassandra.cluster import Cluster
import requests
import base64
import streamlit as st

def run():
    cluster = Cluster(['127.0.0.1'],port=9042)
    session = cluster.connect('spark_streams')

    rows = session.execute("SELECT title, content, image FROM blog_posts")
    posts = []
    for row in rows:
        title = row.title
        content = row.content
        image_url = row.image
        response = requests.get(image_url)
        image = base64.b64encode(response.content).decode('utf-8')
        posts.append((title, content, image))

    for post in posts:
        st.image(post[2], caption=post[0], width=300, use_column_width=False)
        st.write(post[1])