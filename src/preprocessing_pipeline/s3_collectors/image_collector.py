from __future__ import annotations

import pathlib
from dataclasses import dataclass
from io import BytesIO
from urllib.parse import quote

import boto3
import numpy as np
import pandas as pd
import requests
import yaml
from crowd_sdk.cloud import Clouds
from PIL import Image


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


class S3ImagesCollector:
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

    def get_url_and_name(
        self,
        folder_name: str,
        bucket_name: str,
    ) -> pd.DataFrame:
        if folder_name.endswith('/'):
            folder_name = folder_name.rstrip('/')

        files: list[list[str]] = self._s3_client_sdk.walk(
            bucket=bucket_name,
            path=folder_name,
        )

        url_and_filename_dct: dict[str, str] = {'INPUT:image': [], 'FILENAME': [], 'Full_path': []}

        for file in files:
            if file[0] == '' or file[0].split('/')[-1] == '':
                continue

            name: str = file[0]
            folder: str = quote(f'{bucket_name}/{folder_name}/{name}')

            url: str = f'{self._config.cloud.endpoint}/{folder}'
            url_and_filename_dct['INPUT:image'].append(url)
            url_and_filename_dct['FILENAME'].append(name)
            url_and_filename_dct['Full_path'].append(file[1])

        url_and_filename_df = pd.DataFrame(url_and_filename_dct)

        return url_and_filename_df

    def get_image_and_name(
        self,
        folder_name: str,
        bucket_name: str,
    ) -> list[tuple[np.ndarray, str]]:
        url_and_filename_df: pd.DataFrame = self.get_url_and_name(folder_name, bucket_name)

        image_and_filename_dct = {'Image': [], 'Filename': []}

        for index in range(url_and_filename_df.shape[0]):
            current_row: pd.Series = url_and_filename_df.loc[index]

            url = self._s3_client_boto.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': current_row['Full_path'],
                },
                ExpiresIn=3600,
            )

            response = requests.get(url, timeout=60)
            img = np.asarray(Image.open(BytesIO(response.content)))

            image_and_filename_dct['Image'].append(img)
            image_and_filename_dct['Filename'].append(current_row['FILENAME'])

        image_and_filename_lst: list[tuple[np.ndarray, str]] = []

        for image, filename in zip(
            image_and_filename_dct['Image'],
            image_and_filename_dct['Filename'],
        ):
            image_and_filename_lst.append((image, filename))

        return image_and_filename_lst

    def get_and_save_images(
        self,
        s3_bucket_name: str,
        s3_folder_name: str,
        dump_folder_name: str,
    ) -> None:
        url_and_filename_df: pd.DataFrame = self.get_url_and_name(s3_folder_name, s3_bucket_name)

        for index in range(url_and_filename_df.shape[0]):
            current_row: pd.Series = url_and_filename_df.loc[index]

            url = self._s3_client_boto.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': s3_bucket_name,
                    'Key': current_row['Full_path'],
                },
                ExpiresIn=3600,
            )

            response = requests.get(url, timeout=60)

            img: np.ndarray = np.asarray(Image.open(BytesIO(response.content)))
            file_name: str = current_row['FILENAME']

            if '/' in file_name:
                file_name_dir: str = '/'.join(file_name.split('/')[:-1])
                pathlib.Path(f'{dump_folder_name}').joinpath(f'{file_name_dir}').mkdir(parents=True, exist_ok=True)
            else:
                pathlib.Path(f'{dump_folder_name}').mkdir(parents=True, exist_ok=True)

            Image.fromarray(img).save(pathlib.Path(f'{dump_folder_name}').joinpath(file_name))
