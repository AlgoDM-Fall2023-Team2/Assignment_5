# Tag Generator

We have a bunch of images in our s3 bucket. We want to parse all these images to gpt-4 so that it can generate tags for each one of them and then produce a json response. We want to upsert this json to snowflake.

Firstly, we will create a database, schema and a table in snowflake.

```
CREATE SCHEMA IF NOT EXISTS my_schema

CREATE OR REPLACE TABLE my_schema.my_table (
        s3_key STRING,
        tags STRING
    )
```

Now we need to go through each image in our bucket, run it through GPT 4, generate tags and store and generate a list of dictionaries(json).

```
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
```

Finally, we need to upsert these into the snowflake database

```
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
```

After this if we go to our snowflake and observe, we can see that our table has been created with s3_key and tags

![image](https://github.com/AlgoDM-Fall2023-Team2/Assignment_5/assets/39706219/6e930903-d488-4701-949f-71ca0b6c2d29)
