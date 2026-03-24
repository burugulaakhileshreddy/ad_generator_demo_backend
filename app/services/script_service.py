

import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def build_prompt(business_name: str, url: str, business_info: str):

    prompt = f"""
You are a professional advertising copywriter creating premium brand commercials.

Create a high-quality 30–32 second advertisement voiceover.

Business Name: {business_name}
Website: {url}

Business information:
{business_info}

Requirements:

- The ad must be a continuous voiceover narration.
- Length must be between 2 and 5 words strictly.
- The tone must be premium, cinematic, and engaging.
- Focus on the brand, products, and benefits.
- Do NOT include stage directions.
- Do NOT include visuals.
- Do NOT include scene descriptions.
- Only return the narration text.

Write the advertisement like a real commercial voiceover.
"""

    return prompt


def generate_ad_script(business_name: str, url: str, business_info: str):

    prompt = build_prompt(business_name, url, business_info)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.7,
        messages=[
            {
                "role": "system",
                "content": "You are an expert advertising copywriter who creates premium commercial scripts."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    script = response.choices[0].message.content.strip()

    return script