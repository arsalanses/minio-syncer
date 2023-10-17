from minio.error import S3Error
from dotenv import load_dotenv
from minio import Minio
from csv import reader
from os import getenv

load_dotenv()

client = Minio(
    endpoint=getenv('MINIO_HOST'),
    access_key=getenv('ACCESS_KEY'),
    secret_key=getenv('SECRET_KEY'),
)

bucket_name = getenv('BUCKET_NAME')
local_file_path = getenv('LOCAL_FILE')
csv_data_path = getenv('CSV_DATA')

counter = 0
with open(csv_data_path, mode='r') as csv_file:
    csv_reader = reader(csv_file, delimiter=',')
    for row in csv_reader:
        try:
            client.fput_object(bucket_name, "public/" + row[1] + "/" + row[0].split("/")[-1], local_file_path + row[0])
            print(f'public/{row[1]}/{row[0].split("/")[-1]} <= {local_file_path}{row[0]}')
            counter += 1
            print(counter)
        except S3Error as e:
            print(f"Error uploading file: {e}")
