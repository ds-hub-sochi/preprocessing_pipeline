import glob
import pathlib

import numpy as np
import pandas as pd
import pytest
from PIL import Image

from src.preprocessing_pipeline.s3_collectors import image_collector

cwd: pathlib.Path = pathlib.Path(__file__).parent.resolve()


test_data = (
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_nested/'),
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_nested'),
)


@pytest.mark.parametrize('bucket_name,folder_name', test_data)
def test_nested_structure_dir_get_url_and_name(bucket_name: str, folder_name: str):
    img_collector: image_collector.S3ImagesCollector = image_collector.S3ImagesCollector(
        cwd.joinpath('crowd_dump.cfg'),
    )

    urls_and_names: pd.DataFrame = img_collector.get_url_and_name(
        folder_name=folder_name,
        bucket_name=bucket_name,
    )

    answer: pd.DataFrame = pd.read_csv(cwd.joinpath(f'./dumps/url_and_name/{folder_name.rstrip("/")}.csv'))

    assert urls_and_names.equals(answer)


test_data = (
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_flat/'),
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_flat'),
)


@pytest.mark.parametrize('bucket_name,folder_name', test_data)
def test_flat_structure_dir_get_url_and_name(bucket_name: str, folder_name: str):
    img_collector: image_collector.S3ImagesCollector = image_collector.S3ImagesCollector(
        cwd.joinpath('crowd_dump.cfg'),
    )

    urls_and_names: pd.DataFrame = img_collector.get_url_and_name(
        folder_name=folder_name,
        bucket_name=bucket_name,
    )

    answer: pd.DataFrame = pd.read_csv(cwd.joinpath(f'./dumps/url_and_name/{folder_name.rstrip("/")}.csv'))

    assert urls_and_names.equals(answer)


test_data = (
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_nested/'),
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_nested'),
)


@pytest.mark.parametrize('bucket_name,folder_name', test_data)
def test_nested_structure_dir_get_image_and_name(bucket_name: str, folder_name: str):
    img_collector: image_collector.S3ImagesCollector = image_collector.S3ImagesCollector(
        cwd.joinpath('crowd_dump.cfg'),
    )

    images_and_names: list[tuple[np.ndarray, str]] = img_collector.get_image_and_name(
        folder_name=folder_name,
        bucket_name=bucket_name,
    )
    images_and_names = sorted(images_and_names, key=lambda x: x[1])

    answer_images: list[str] = glob.glob(str(cwd.joinpath('dumps/image_and_name/nested/**/*.JPG')), recursive=True)
    answer_images = sorted(answer_images)

    for index, filename in enumerate(answer_images):
        image: np.ndarray = np.asarray(Image.open(filename))
        filename: str = '/'.join(answer_images[index].replace('\\', '/').split('/')[-2:])

        assert np.allclose(images_and_names[index][0], image)
        assert images_and_names[index][1] == filename


test_data = (
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_flat/'),
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_flat'),
)


@pytest.mark.parametrize('bucket_name,folder_name', test_data)
def test_flat_structure_dir_get_image_and_name(bucket_name: str, folder_name: str):
    img_collector: image_collector.S3ImagesCollector = image_collector.S3ImagesCollector(
        cwd.joinpath('crowd_dump.cfg'),
    )

    images_and_names: list[tuple[np.ndarray, str]] = img_collector.get_image_and_name(
        folder_name=folder_name,
        bucket_name=bucket_name,
    )
    images_and_names = sorted(images_and_names, key=lambda x: x[1])

    answer_images: list[str] = glob.glob(str(cwd.joinpath('dumps/image_and_name/flat/**/*.JPG')), recursive=True)
    answer_images = sorted(answer_images)

    for index, filename in enumerate(answer_images):
        image_answer = np.asarray(Image.open(filename))

        images_from_s3_np: np.ndarray = images_and_names[index][0]

        filename: str = answer_images[index].replace('\\', '/').split('/')[-1]

        assert images_from_s3_np.shape == image_answer.shape
        assert np.allclose(images_from_s3_np, image_answer)
        assert images_and_names[index][1] == filename
