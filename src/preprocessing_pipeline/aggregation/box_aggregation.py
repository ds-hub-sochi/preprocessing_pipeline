from __future__ import annotations

import json
import os
import os.path
import pathlib
import typing
from collections import Counter
from dataclasses import dataclass

import numpy as np
import pandas as pd
from PIL import Image
from sklearn.cluster import MeanShift
from tqdm import tqdm


@dataclass
class Bbox:
    x: int
    y: int
    width: int
    height: int


@dataclass
class Answer:
    label: str
    position: Bbox

    def __post_init__(self):
        self.position = Bbox(**self.position)  # pyling: disable=not-a-mapping

    def position_to_dict(self) -> dict[str, int]:
        return {
            'x1': self.position.x,
            'y1': self.position.y,
            'x2': self.position.x + self.position.width,
            'y2': self.position.y + self.position.height,
        }


class BoxAggregator:
    def __init__(
        self,
        path_to_images_dir: str | pathlib.Path,
        path_to_wrong_cases_dir: str | pathlib.Path,
        images_dir_structure: str = 'nested',
        tagme_markup_structure: str = 'nested',
    ):
        self.path_to_images_dir: str | pathlib.Path = os.path.abspath(path_to_images_dir)
        self.path_to_wrong_cases_dir: str | pathlib.Path = os.path.abspath(path_to_wrong_cases_dir)
        self.images_dir_structure: str = images_dir_structure
        self.tagme_markup_structure: str = tagme_markup_structure

    def aggregate(
        self,
        path_to_markup: str | pathlib.Path,
        bbox_minimal_relative_size: float = 0.005,
        bbox_relative_error: float = 0.01,
    ) -> pd.DataFrame:
        with open(path_to_markup, 'r', encoding='utf-8') as json_file:
            markup_json = json.load(json_file)

        markup_filtered_by_label = self.filter_markup_by_label(
            markup_json,
        )

        markup_filtered_by_bbox_size = self.filted_by_bbox_size(
            markup_filtered_by_label,
            bbox_minimal_relative_size=bbox_minimal_relative_size,
        )

        markup_filtered_by_consistency = self.filter_markup_by_consistency(
            markup_filtered_by_bbox_size,
            bbox_relative_error=bbox_relative_error,
        )

        return markup_filtered_by_consistency

    def filter_markup_by_label(
        self,
        markup_json: dict[str, typing.Any],
    ) -> dict[str, typing.Any]:
        markup_with_filtered_labels = []

        bad_markup: dict[str, list[str]] = {
            'filepath': [],
            'reason': [],
        }
        empty_files: set[str] = set()

        for sample in markup_json:
            if 'result' in sample:
                correct_marks = []
                got_mistake: bool = False

                for mark in sample['result']['marks']:
                    if mark['entityId'] == 'empty' or mark['entityId'] == 'Bad_quality':
                        if sample['file_name'] not in empty_files:
                            bad_markup['filepath'].append(sample['file_name'])
                            bad_markup['reason'].append(mark['entityId'])
                            correct_marks = []
                    else:
                        if mark['entityId'] == 'mistake':
                            got_mistake = True
                            continue
                        correct_marks.append(mark)
                if len(correct_marks) != 0:
                    markup_with_filtered_labels.append(sample)
                    markup_with_filtered_labels[-1]['result']['marks'] = correct_marks

                if got_mistake and len(correct_marks) == 0:
                    bad_markup['filepath'].append(sample['file_name'])
                    bad_markup['reason'].append('all_mistakes')
        pd.DataFrame.from_dict(bad_markup).to_csv(f'{self.path_to_wrong_cases_dir}/filtered_files.csv')

        return markup_with_filtered_labels

    def filted_by_bbox_size(
        self,
        markup_with_filtered_labels: dict[str, typing.Any],
        bbox_minimal_relative_size: float = 0.005,
    ):
        markers_predictions: dict[str, dict[str, list[Answer]]] = {}
        wrond_bboxes: dict[str, list[str]] = {
            'filepath': [],
            'marker_id': [],
            'bbox_x': [],
            'bbox_y': [],
            'bbox_width': [],
            'bbox_height': [],
        }

        for sample in markup_with_filtered_labels:
            filename: str = sample['file_name']
            current_marker_id: str = sample['marker_id']

            if 'result' in sample:
                if filename not in markers_predictions:
                    markers_predictions[filename] = {}

                if current_marker_id not in markers_predictions[filename]:
                    markers_predictions[filename][current_marker_id] = []

                for current_result in sample['result']['marks']:
                    current_bbox: dict[str, int] = {
                        'x': current_result['position']['x'],
                        'y': current_result['position']['y'],
                        'width': current_result['position']['width'],
                        'height': current_result['position']['height'],
                    }

                    filename_restructured = filename

                    if self.images_dir_structure != self.tagme_markup_structure:
                        if self.tagme_markup_structure == 'nested':
                            filename_restructured: str = filename.replace('/', '_')
                        else:
                            filename_restructured: str = filename.replace('_', '/')

                    current_image: np.ndarray = np.asarray(
                        Image.open(f'{self.path_to_images_dir}/{filename_restructured}'),
                    )
                    width, height, _ = current_image.shape

                    minimal_width: int = round(width * bbox_minimal_relative_size)
                    minimal_height: int = round(height * bbox_minimal_relative_size)

                    if current_bbox['width'] > minimal_width and current_bbox['height'] > minimal_height:
                        markers_predictions[filename][current_marker_id].append(
                            Answer(current_result['entityId'], current_bbox),
                        )
                    else:
                        wrond_bboxes['filepath'].append(filename)
                        wrond_bboxes['marker_id'].append(current_marker_id)
                        wrond_bboxes['bbox_x'].append(current_bbox['x'])
                        wrond_bboxes['bbox_y'].append(current_bbox['y'])
                        wrond_bboxes['bbox_width'].append(current_bbox['width'])
                        wrond_bboxes['bbox_height'].append(current_bbox['height'])
        pd.DataFrame.from_dict(wrond_bboxes).to_csv(f'{self.path_to_wrong_cases_dir}/wrond_bboxes.csv')

        return markers_predictions

    def filter_markup_by_consistency(  # pylint: disable=[too-many-locals,too-many-statements]
        self,
        markers_predictions: dict[str, typing.Any],
        bbox_relative_error: float = 0.01,
    ) -> pd.DataFrame:
        aggregation_dct: dict[str, list[str]] = {
            'subtask': [],
            'task': [],
            'marker_id': [],
            'label': [],
            'bbox_x': [],
            'bbox_y': [],
            'bbox_width': [],
            'bbox_height': [],
        }

        inconsistent_bboxes: dict[str, list[str]] = {
            'task': [],
            'marker_id': [],
            'bbox_x': [],
            'bbox_y': [],
            'bbox_width': [],
            'bbox_height': [],
        }

        for task in tqdm(markers_predictions):
            current_task_predictions: dict[str, list[Answer]] = markers_predictions[task]
            lens: list[int] = []

            for marker_id in current_task_predictions:
                lens.append(len(current_task_predictions[marker_id]))

            vals, counts = np.unique(lens, return_counts=True)
            index: int = np.argmax(counts)
            n_clusters: int = vals[index]

            centers: list[tuple[int, int]] = []
            workers: list[str] = []
            bboxes: list[Answer] = []

            for marker_id in current_task_predictions:
                for prediction in current_task_predictions[marker_id]:
                    y_center: int = round(prediction.position.y + prediction.position.height / 2)
                    x_center: int = round(prediction.position.x + prediction.position.width / 2)

                    centers.append((x_center, y_center))
                    workers.append(marker_id)
                    bboxes.append(prediction)

            filename_restructured: str = task
            if self.images_dir_structure != self.tagme_markup_structure:
                if self.tagme_markup_structure == 'nested':
                    filename_restructured: str = task.replace('/', '_')
                else:
                    filename_restructured: str = task.replace('_', '/')

            current_image: np.ndarray = np.asarray(Image.open(f'{self.path_to_images_dir}/{filename_restructured}'))
            width, height, _ = current_image.shape

            bandwidth = round(np.sqrt((width * bbox_relative_error) ** 2 + (height * bbox_relative_error) ** 2))

            mean_shift: MeanShift = MeanShift(bandwidth=bandwidth)
            mean_shift.fit(centers)
            clusters: np.array = mean_shift.labels_

            label_counter = Counter()

            for label in clusters:
                if label not in label_counter:
                    label_counter[label] = 0
                label_counter[label] += 1

            for predicted_cluster, worker, answer in zip(clusters, workers, bboxes):
                got_label = False
                for subtask_number, (label, _) in enumerate(label_counter.most_common(n_clusters)):
                    if predicted_cluster == label:
                        aggregation_dct['subtask'].append(f'{task}_{subtask_number}')
                        aggregation_dct['task'].append(task)
                        aggregation_dct['marker_id'].append(worker)
                        aggregation_dct['label'].append(answer.label)
                        aggregation_dct['bbox_x'].append(answer.position.x)
                        aggregation_dct['bbox_y'].append(answer.position.y)
                        aggregation_dct['bbox_width'].append(answer.position.width)
                        aggregation_dct['bbox_height'].append(answer.position.height)
                        got_label = True
                        break

                    if not got_label:
                        inconsistent_bboxes['task'].append(f'{task}')
                        inconsistent_bboxes['marker_id'].append(worker)
                        inconsistent_bboxes['bbox_x'].append(answer.position.x)
                        inconsistent_bboxes['bbox_y'].append(answer.position.y)
                        inconsistent_bboxes['bbox_width'].append(answer.position.width)
                        inconsistent_bboxes['bbox_height'].append(answer.position.height)

        pd.DataFrame.from_dict(inconsistent_bboxes).to_csv(f'{self.path_to_wrong_cases_dir}/inconsistent_bboxes.csv')

        return pd.DataFrame.from_dict(aggregation_dct)


if __name__ == '__main__':
    a = BoxAggregator(
        './horse',
        './tmp',
        'flat',
        'flat',
    )

    res = a.aggregate(
        './Детекция животных_horse_validate.json',
    )
    print(res.head())
