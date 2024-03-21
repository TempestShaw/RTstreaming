import json
from openai import OpenAI
from dotenv import load_dotenv
import os
import logging

def call_agents(prompt=None, text=None, image_url=None, model=None, temperature=0.7):
    load_dotenv(verbose=True)
    api_key = os.environ.get("OPENAI_API_KEY")
    client = OpenAI(
        api_key=api_key,
    )
    content = []
    if image_url:
        response = call_vision_agent(prompt=prompt, text=text,
                                     image_url=image_url, model=model, temperature=temperature)
        return response.choices[0].message.content
    message = [
        {
            "role": "system",
            "content": prompt
        },
        {
            "role": "user",
            "content": text,
        }
    ]
    logging.info(f"Call agents with message: \n\n\n\n{message}\n\n\n\n")
    logging.info(f"Call agents with model: {model}")
    logging.info(f"Call agents with temperature: {temperature}")
    chat_completion = client.chat.completions.create(
        messages=message,
        model=model,
        temperature=temperature,
        max_tokens=2048
    )
    return chat_completion.choices[0].message.content


def call_vision_agent(prompt=None, text=None, image_url=None, model=None, temperature=0.7):
    client = OpenAI(
        # This is the default and can be omitted
        api_key=os.environ.get("OPENAI_API_KEY"),
    )
    response = client.chat.completions.create(
        model=model,
        temperature=temperature,
        messages=[
            {
                "role": "system",
                "content": prompt
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": text},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": image_url,
                        },
                    },
                ],
            }
        ],
        max_tokens=2048,
    )
    return response

def json_parsing(jsonLikeStr):
    jsonLikeStr = jsonLikeStr[jsonLikeStr.find('{'):jsonLikeStr.find('}')+1]
    data = json.loads(jsonLikeStr)
    return data
