import os
from os.path import join, dirname

from dotenv import load_dotenv
from minio import Minio

media_types = ['audio', 'files', 'gifs', 'photos', 'videos']

dotenv_path = join(dirname(__file__), '../.env')
load_dotenv(dotenv_path)

api_host = os.environ.get('MINIO_API_HOST')
storage_host = os.environ.get('MINIO_STORAGE_HOST')

client = Minio(api_host, os.environ.get('MINIO_ACCESS_KEY'), os.environ.get('MINIO_SECRET_KEY'), False)


def upload(conversation):
    bucket_name = conversation.replace('_', '-')
    _init(bucket_name)
    folder = f"./../data/messages/inbox/{conversation}"
    for media_type in media_types:
        for file in os.listdir(f"{folder}/{media_type}"):
            print('Upload file : ', file)
            client.fput_object(bucket_name, f"{media_type}/{file}", f"{folder}/{media_type}/{file}")


def get_url(uri):
    [bucket_name, object_name] = uri.replace('messages/inbox/', '').split('/', 1)

    return "https://" + "{host}/{bucket_name}/{object_name}".format(
        host=storage_host,
        bucket_name=bucket_name.replace('_', '-'),
        object_name=object_name
    ).replace('//', '/')


def _init(name):
    if client.bucket_exists(name):
        print('Bucket already exist.')
    else:
        print('Create bucket : ', name)
        client.make_bucket(name)
