from openai import OpenAI
import openai
import boto3
import os

api_key = "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"
openai.api_key = api_key

os.environ["OPENAI_API_KEY"] = api_key

client = OpenAI()

aws_access_key = "AKIAUCVRRS2K6T4FC27T"
aws_secret_key = "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"
aws_region = "us-east-1"

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)

objects = s3_client.list_objects_v2(Bucket = "smalladmbucket", StartAfter = '/img')

for object in objects['Contents']:

    presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': "smalladmbucket", 'Key': object['Key']}, ExpiresIn=3600)

    tags = ["men", "women", "Long sleeve", "Truck", "Online shopping", "hello world"]

    response = client.chat.completions.create(
    model="gpt-4-vision-preview",
    messages=[
        {
        "role": "user",
        "content": [
            {"type": "text", "text": f"From the following tags {tags} , choose top3 most appropriate for the following image. Just give me the tags seperated by comma"},
            {
            "type": "image_url",
            "image_url": {
                "url": presigned_url,
                },
            },
        ],
        }
    ],
    max_tokens=300,
    )

    print(response.choices[0].message.content)