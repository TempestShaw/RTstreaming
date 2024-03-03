from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from static.prompt import *
from utils.scrapping import *  # Assuming Scrapper is defined in this module
from utils.utils import *
import logging
import asyncio

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}
# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


async def scrapping(website):
    scraper = Scraper()
    return await scraper.scrap_from_website(website)

def sudo_api():
    data = {"title": "\\ud83c\\udf0d2023\\u5e74\\u5168\\u7403\\u6700\\u591a\\u4eba\\u8bf4\\u7684\\u8bed\\u8a00\\u6392\\u884c\\u699c\\uff0c\\u4f60\\u4f1a\\u51e0\\u79cd\\uff1f\\ud83d\\udd1d", "content": "\\ud83d\\udcca\\u5728\\u8fd9\\u4e2a\\u5168\\u7403\\u5316\\u7684\\u65f6\\u4ee3\\uff0c\\u4e86\\u89e3\\u4e16\\u754c\\u4e0a\\u6700\\u591a\\u4eba\\u4f7f\\u7528\\u7684\\u8bed\\u8a00\\u4e0d\\u4ec5\\u80fd\\u5e2e\\u52a9\\u6211\\u4eec\\u66f4\\u597d\\u5730\\u4e0e\\u4ed6\\u4eba\\u6c9f\\u901a\\uff0c\\u8fd8\\u80fd\\u8ba9\\u6211\\u4eec\\u5728\\u804c\\u573a\\u4e0a\\u66f4\\u5177\\u7ade\\u4e89\\u529b\\u3002\\u4eca\\u5929\\uff0c\\u5c31\\u8ba9\\u6211\\u4eec\\u4e00\\u8d77\\u6765\\u770b\\u770b2023\\u5e74\\u5168\\u7403\\u6700\\u591a\\u4eba\\u8bf4\\u7684\\u8bed\\u8a00\\u6392\\u884c\\u699c\\u5427\\uff01\\ud83c\\udf10\\n\\n\\ud83e\\udd47**\\u82f1\\u8bed\\u4f9d\\u65e7\\u5360\\u636e\\u699c\\u9996**\\n\\u82f1\\u8bed\\u4e0d\\u4ec5\\u662f\\u5546\\u4e1a\\u548c\\u653f\\u6cbb\\u7684\\u56fd\\u9645\\u901a\\u7528\\u8bed\\uff0c\\u8fd8\\u662f2023\\u5e74\\u5168\\u7403\\u6700\\u591a\\u4eba\\u4f7f\\u7528\\u7684\\u8bed\\u8a00\\uff0c\\u4f7f\\u7528\\u4eba\\u6570\\u8fbe\\u5230\\u4e86\\u60ca\\u4eba\\u768415\\u4ebf\\uff01\\u8fd9\\u4e5f\\u8bc1\\u660e\\u4e86\\u82f1\\u8bed\\u4f5c\\u4e3a\\u4e00\\u79cd\\u5168\\u7403\\u8bed\\u8a00\\u7684\\u5730\\u4f4d\\u3002\\ud83c\\udf0e\\n\\n\\ud83e\\udd48**\\u666e\\u901a\\u8bdd\\u7d27\\u968f\\u5176\\u540e**\\n\\u4f5c\\u4e3a\\u4e2d\\u56fd\\u548c\\u53f0\\u6e7e\\u6700\\u591a\\u4eba\\u8bf4\\u7684\\u8bed\\u8a00\\uff0c\\u666e\\u901a\\u8bdd\\u4ee511\\u4ebf\\u7684\\u4f7f\\u7528\\u8005\\u4f4d\\u5217\\u7b2c\\u4e8c\\u3002\\u8fd9\\u4e2a\\u6570\\u5b57\\u8fd8\\u4e0d\\u5305\\u62ec\\u5206\\u5e03\\u5728\\u4e1c\\u5357\\u4e9a\\u53ca\\u5168\\u7403\\u5176\\u4ed6\\u5730\\u533a\\u7684\\u6570\\u767e\\u4e07\\u4f7f\\u7528\\u8005\\u3002\\ud83c\\udde8\\ud83c\\uddf3\\n\\n\\ud83e\\udd49**\\u897f\\u73ed\\u7259\\u8bed\\u548c\\u5370\\u5730\\u8bed\\u7684\\u5f3a\\u52bf\\u8868\\u73b0**\\n\\u897f\\u73ed\\u7259\\u8bed\\u548c\\u5370\\u5730\\u8bed\\u5206\\u522b\\u4ee55.59\\u4ebf\\u548c6.1\\u4ebf\\u7684\\u4f7f\\u7528\\u8005\\u6570\\u91cf\\uff0c\\u4f4d\\u5217\\u7b2c\\u4e09\\u548c\\u7b2c\\u56db\\u3002\\u8fd9\\u4e24\\u79cd\\u8bed\\u8a00\\u7684\\u5e7f\\u6cdb\\u4f7f\\u7528\\u5c55\\u793a\\u4e86\\u5b83\\u4eec\\u5404\\u81ea\\u6587\\u5316\\u7684\\u5f71\\u54cd\\u529b\\u3002\\ud83c\\udf0d\\n\\n\\ud83d\\udcc8**\\u6cd5\\u8bed\\u548c\\u963f\\u62c9\\u4f2f\\u8bed\\u4e5f\\u4e0d\\u5bb9\\u5c0f\\u89d1**\\n\\u6cd5\\u8bed\\u548c\\u6807\\u51c6\\u963f\\u62c9\\u4f2f\\u8bed\\u5206\\u522b\\u67093.1\\u4ebf\\u548c2.74\\u4ebf\\u7684\\u4f7f\\u7528\\u8005\\uff0c\\u5c55\\u73b0\\u4e86\\u5b83\\u4eec\\u5728\\u5168\\u7403\\u8bed\\u8a00\\u4e2d\\u7684\\u91cd\\u8981\\u5730\\u4f4d\\u3002\\ud83d\\udde3\\n\\n\\ud83d\\udd0d\\u8fd9\\u4efd\\u699c\\u5355\\u4e0d\\u4ec5\\u53cd\\u6620\\u4e86\\u7ecf\\u6d4e\\u8d8b\\u52bf\\u3001\\u4eba\\u53e3\\u5bc6\\u96c6\\u56fd\\u5bb6\\uff0c\\u8fd8\\u6709\\u6b96\\u6c11\\u5386\\u53f2\\u7684\\u5f71\\u54cd\\u3002\\u4ece\\u82f1\\u8bed\\u5230\\u666e\\u901a\\u8bdd\\uff0c\\u518d\\u5230\\u897f\\u73ed\\u7259\\u8bed\\u548c\\u5370\\u5730\\u8bed\\uff0c\\u6bcf\\u4e00\\u79cd\\u8bed\\u8a00\\u80cc\\u540e\\u90fd\\u6709\\u7740\\u4e30\\u5bcc\\u7684\\u6587\\u5316\\u548c\\u5386\\u53f2\\u3002\\ud83d\\udcda\\n\\n\\u4f60\\u4f1a\\u54ea\\u51e0\\u79cd\\u8bed\\u8a00\\u5462\\uff1f\\u5feb\\u6765\\u8bc4\\u8bba\\u533a\\u5206\\u4eab\\u4f60\\u7684\\u8bed\\u8a00\\u5b66\\u4e60\\u7ecf\\u5386\\u5427\\uff01\\ud83d\\udc47\\n\\n#\\u5168\\u7403\\u8bed\\u8a00\\u6392\\u884c\\u699c #\\u8bed\\u8a00\\u5b66\\u4e60 #\\u591a\\u8bed\\u79cd #\\u82f1\\u8bed #\\u666e\\u901a\\u8bdd #\\u897f\\u73ed\\u7259\\u8bed #\\u5370\\u5730\\u8bed #\\u6cd5\\u8bed #\\u963f\\u62c9\\u4f2f\\u8bed", "image": "https://www.visualcapitalist.com/wp-content/uploads/2024/02/Most-Spoken-Languages-update.jpg"}
    return data
def process_raw_data(website):
    title, content, image_src = asyncio.run(scrapping(website))
    if image_src:
        image_description = call_agents(
            prompt=dataprompt,
            text="请根据图片描述。",
            image_url=image_src,
            model='gpt-4-vision-preview',
            temperature=0.3)

        logging.info(f"Image description: {image_description}")

        redreturn = call_agents(
            prompt=redprompt,
            text=f"""
                标题:{title}
                内容:{content}
                    {image_description}""",
            model='gpt-4-0125-preview',
            temperature=0.3)

        logging.info(f"Red return: {redreturn}")

        data = json_parsing(redreturn)

        title = data["标题"]
        content = data["稿子正文"]
        data = {"title": title, "content": content,"image": image_src}
        return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    try:
        data = process_raw_data(visualCapitalist)
            # ##write a functio break after 60 seconds
            # if time.time() - curr_time > 60:
            #
            #     break
        # data = sudo_api()
        producer.send('blog_posts', json.dumps(data).encode('utf-8'))
    except Exception as e:
        logging.error(f'An error occured: {e}')

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )


# pytest-playwright==0.4.4
# openai==1.13.3
# colorlog==4.8.0