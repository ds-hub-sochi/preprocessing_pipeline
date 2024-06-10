import csv
import json
import os
import pathlib
import typing

import pandas as pd
from crowd_sdk.tagme import TagmeClientAdvanced
from cv2 import cv2

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


def df_to_tagme_json(
    path_to_aggregation_df: str,
    path_to_tagme_json: str,
    dump_dir: str,
):  # pylint: disable=[too-many-locals]
    aggregation_df: pd.DataFrame = pd.read_csv(path_to_aggregation_df)

    with open(path_to_tagme_json, 'r', encoding='utf-8') as json_path:
        tagme_markup = json.load(json_path)

    filename_and_marker_id2markup: dict[tuple[str, str], typing.Any] = {}

    for sample in tagme_markup:
        filename_and_marker_id2markup[(sample['file_name'], sample['marker_id'])] = sample

    result_json = []

    tasks = list(set(aggregation_df['task']))
    for task in tasks:
        task_markups = aggregation_df[aggregation_df['task'] == task]
        marks = []
        for x, y, w, h, label in zip(
            task_markups['bbox_x'],
            task_markups['bbox_y'],
            task_markups['bbox_width'],
            task_markups['bbox_height'],
            task_markups['label'],
        ):
            current_mark = {
                'type': 'bbox',
                'entityId': label,
                'position': {
                    'x': x,
                    'y': y,
                    'width': w,
                    'height': h,
                    'rotation': 0,
                },
            }
            marks.append(current_mark)

        marker_id: str = list(task_markups['marker_id'])[0]

        original_markup = filename_and_marker_id2markup[(task, marker_id)]

        current_markup = {'result': {'marks': marks}}

        for key in list(original_markup.keys()):
            if key != 'result':
                current_markup[key] = original_markup[key]

        result_json.append(current_markup)

    with open(f'{dump_dir}/aggragation_in_tagme_format.json', 'w', encoding='utf-8') as outfile:
        json.dump(result_json, outfile)


def draw_markup(
    path_to_tagme_json,
    images_dir,
    dump_dir,
    images_dir_structure: str,
    tagme_markup_structure: str,
):
    with open(path_to_tagme_json, 'r', encoding='utf-8') as markup_file:
        markup = json.load(markup_file)

    color = (255, 0, 0)

    for sample in markup:
        filename = sample['file_name']

        if images_dir_structure != tagme_markup_structure:
            if images_dir_structure == 'nested':
                image = cv2.imread(f'{images_dir}/{filename.replace("_", "/")}')
            else:
                image = cv2.imread(f'{images_dir}/{filename.replace("/", "_")}')
        else:
            image = cv2.imread(f'{images_dir}/{filename}')

        for mark in sample['result']['marks']:
            x = mark['position']['x']
            y = mark['position']['y']
            w = mark['position']['width']
            h = mark['position']['height']
            label = mark['entityId']

            image = cv2.rectangle(image, (x, y), (x + w, y + h), color, 3)
            cv2.putText(image, label, (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (36, 255, 12), 2)

        pathlib.Path(f'{dump_dir}').mkdir(parents=True, exist_ok=True)

        cv2.imwrite(f'{dump_dir}/{filename}', image)
