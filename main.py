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

sizes = ['120x120', '150x150', '300x150', '300x169', '300x199', '300x200', '300x204', '300x230', '768x384', '768x432', '1024x512']
counter = 0
with open(csv_data_path, mode='r') as csv_file:
    csv_reader = reader(csv_file, delimiter=',')
    for row in csv_reader:
        object_name = "public/" + row[1] + "/" + row[0].split("/")[-1]
        file_path = local_file_path + row[0]
        try:
            client.fput_object(bucket_name, object_name, file_path)
            for size in sizes:
                object_name_with_size = object_name[:-4] + '-' + size + object_name[-4:]
                file_path_with_size = file_path[:-4] + '-' + size + file_path[-4:]
                try:
                    client.fput_object(bucket_name, object_name_with_size, file_path_with_size)
                except S3Error as e:
                    print(f"Error uploading file: {e}")
                except FileNotFoundError:
                    print("FileNotFoundError")
                print(f'{counter} \t {object_name_with_size} <= {file_path_with_size}')
                counter += 1
        except S3Error as e:
            print(f"Error uploading file: {e}")
        except FileNotFoundError:
            print("FileNotFoundError")
        # else:
        #     print(f'success.')
        # finally:
        #     print(f'round completed.')
