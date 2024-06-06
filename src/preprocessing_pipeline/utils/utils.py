import csv
import os
import pathlib

import pandas as pd
from crowd_sdk.tagme import TagmeClientAdvanced

from src.preprocessing_pipeline.s3_collectors.image_collector import S3ImagesCollector


async def upload_s3_to_tagme(bucket_name: str, folder_name: str, task_id: str, organization_id: str) -> None:
    try:  # pylint: disable=too-many-try-statements
        client: TagmeClientAdvanced = TagmeClientAdvanced()

        crowd_cfg_path: str = str(pathlib.Path.home().joinpath('.crowd.cfg'))
        s3_collector = S3ImagesCollector(crowd_cfg_path)

        temp_dir: pathlib.Path = pathlib.Path('./tmp')
        os.makedirs(temp_dir, exist_ok=True)

        csv_file_path = temp_dir / 'images.csv'

        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            fieldnames = ['INPUT:image', 'FILENAME']

            writer: csv.DictWriter = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            images_df: pd.DataFrame = s3_collector.get_url_and_name(
                bucket_name=bucket_name,
                folder_name=folder_name,
            )

            for i in range(images_df.shape[0]):
                sample: pd.Series = images_df.loc[i]
                writer.writerow({'INPUT:image': sample['INPUT:image'], 'FILENAME': sample['FILENAME']})

        await client.upload_table(task_id, csv_file_path, delimeter=',', organization_id=organization_id)
    except SystemExit:
        pass
