from __future__ import annotations

import os
import pathlib
from dataclasses import dataclass
from glob import glob

import boto3
import yaml
from crowd_sdk.cloud import Clouds
from tqdm import tqdm


@dataclass
class _Tagme:
    url: str
    auth_url: str
    user: str
    password: str


@dataclass
class _S3:
    endpoint: str
    key: str
    secret: str


@dataclass
class Config:
    tagme: _Tagme
    cloud: _S3

    def __init__(self, config_yaml):
        self.tagme = _Tagme(**config_yaml['tagme'])
        self.cloud = _S3(**config_yaml['cloud']['main'])


class S3FileDepositor:
    def __init__(self, path_to_crowd_cfg: str | pathlib.Path):
        self._s3_client_sdk = Clouds(str(path_to_crowd_cfg)).get('main')

        with open(path_to_crowd_cfg, 'r', encoding='utf-8') as config_path:
            crowd_cfg = yaml.safe_load(config_path)

        self._config: Config = Config(crowd_cfg)

        self._s3_client_boto: boto3.client = boto3.client(
            's3',
            endpoint_url=self._config.cloud.endpoint,
            aws_access_key_id=self._config.cloud.key,
            aws_secret_access_key=self._config.cloud.secret,
        )

    def deposit_file(
        self,
        s3_bucket_name: str,
        s3_folder_name: str,
        filepath: str | pathlib.Path,
    ) -> None:
        if not s3_folder_name.endswith('/'):
            s3_folder_name += '/'

        self._s3_client_boto.put_object(
            Bucket=s3_bucket_name,
            Key=(s3_folder_name),
        )

        filename: str = str(filepath).rsplit('/', maxsplit=1)[-1]

        s3_resource: boto3.resource = boto3.resource(
            's3',
            endpoint_url=self._config.cloud.endpoint,
            aws_access_key_id=self._config.cloud.key,
            aws_secret_access_key=self._config.cloud.secret,
        )

        s3_resource.Bucket(s3_bucket_name).upload_file(
            filepath,
            str(pathlib.Path(s3_folder_name).joinpath(filename)),
        )

    def delete_file(
        self,
        s3_bucket_name: str,
        s3_filepath: str,
    ) -> None:
        s3_resource: boto3.resource = boto3.resource(
            's3',
            endpoint_url=self._config.cloud.endpoint,
            aws_access_key_id=self._config.cloud.key,
            aws_secret_access_key=self._config.cloud.secret,
        )

        bucket = s3_resource.Bucket(s3_bucket_name)
        bucket.objects.filter(Prefix=s3_filepath).delete()

    def deposite_folder(
        self,
        s3_bucket_name: str,
        s3_folder_name: str,
        dirpath: str | pathlib.Path,
    ) -> None:
        prefix_len: int = len(os.path.abspath(dirpath))

        dir_files: list[str] = glob(f'{os.path.abspath(dirpath)}/*.*')

        if len(dir_files) == 0:
            dir_files = glob(f'{os.path.abspath(dirpath)}/*/*.*')

        for filepath in tqdm(dir_files):
            filename: str = filepath[prefix_len + 1 :]

            s3_resource: boto3.resource = boto3.resource(
                's3',
                endpoint_url=self._config.cloud.endpoint,
                aws_access_key_id=self._config.cloud.key,
                aws_secret_access_key=self._config.cloud.secret,
            )

            s3_resource.Bucket(s3_bucket_name).upload_file(
                filepath,
                str(pathlib.Path(s3_folder_name).joinpath(filename)),
            )
