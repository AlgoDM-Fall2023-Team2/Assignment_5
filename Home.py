from openai import OpenAI
import boto3
import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

client = OpenAI()

aws_access_key = os.environ.get('aws_access_key')
aws_secret_key = os.environ.get('aws_secret_key')
aws_region = os.environ.get('aws_region')

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)

objects = s3_client.list_objects_v2(Bucket = "smalladmbucket", StartAfter = '/img')

conn = snowflake.connector.connect(
    user=os.environ.get("user"),
    password=os.environ.get("password"),
    account=os.environ.get("account"),
    warehouse=os.environ.get("warehouse"),
    database=os.environ.get("database"),
    schema=os.environ.get("schema")
)

cursor = conn.cursor()

cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS my_schema
""")

cursor.close()

cursor = conn.cursor()

cursor.execute("""
    CREATE OR REPLACE TABLE my_schema.my_table (
        s3_key STRING,
        tags STRING
    )
""")

cursor.close()

responses = []

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

    tags = response.choices[0].message.content

    tags_list = tags.split(sep = ',')

    item = {object['Key']:tags_list}

    responses.append(item)

cursor = conn.cursor()

for dictionary in responses:
    for key, value in dictionary.items():

        values = ','.join(value)
        # Use MERGE INTO to upsert based on the key
        cursor.execute("""
            MERGE INTO my_schema.my_table AS target
            USING (SELECT %s AS s3_key, %s AS tags) AS source
            ON target.s3_key = source.s3_key
            WHEN MATCHED THEN
                UPDATE SET target.tags = source.tags
            WHEN NOT MATCHED THEN
                INSERT (s3_key, tags)
                VALUES (source.s3_key, source.tags)
        """, (key, values))

cursor.close()