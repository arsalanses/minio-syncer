from dotenv import load_dotenv
from minio import Minio
from os import getenv

load_dotenv()

client = Minio(
    str(getenv('MINIO_HOST')),
    access_key=getenv('ACCESS_KEY'),
    secret_key=getenv('SECRET_KEY'),
)

buckets = client.list_buckets()

for bucket in buckets:
    print(bucket)
