import pathlib

import pandas as pd
import pytest

from src.preprocessing_pipeline.s3_collectors import image_collector
from src.preprocessing_pipeline.s3_depositors import file_depositor

cwd: pathlib.Path = pathlib.Path(__file__).parent.resolve()

test_data = (
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_deposit/', 'dumps/test1.txt'),
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_deposit', 'dumps/test1.txt'),
)


@pytest.mark.parametrize('s3_bucket_name,s3_folder_name,filepath', test_data)
def test_one_file_deposit(s3_bucket_name: str, s3_folder_name: str, filepath: str):
    filepath = str(cwd.joinpath(filepath))

    depositor: file_depositor.S3FileDepositor = file_depositor.S3FileDepositor(
        cwd.joinpath('./crowd_dump.cfg'),
    )
    depositor.deposit_file(s3_bucket_name, s3_folder_name, filepath)

    collector: image_collector.S3ImagesCollector = image_collector.S3ImagesCollector(
        cwd.joinpath('./crowd_dump.cfg'),
    )

    files: pd.DataFrame = collector.get_url_and_name(
        bucket_name=s3_bucket_name,
        folder_name=s3_folder_name,
    )

    filename: str = filepath.rsplit('/', maxsplit=1)[-1]

    depositor.delete_file(
        s3_bucket_name=s3_bucket_name,
        s3_filepath=files.loc[0]['Full_path'],
    )

    assert str(pathlib.Path(s3_folder_name).joinpath(filename)) == files.loc[0]['Full_path']


test_data = (
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_deposit_with_existing_file/', 'dumps/test1.txt'),
    ('b-ws-faq3m-mp1', 'preprocessing_pipeline_test_dir_deposit_with_existing_file', 'dumps/test1.txt'),
)


@pytest.mark.parametrize('s3_bucket_name,s3_folder_name,filepath', test_data)
def test_one_file_deposit_into_existing_folder(s3_bucket_name: str, s3_folder_name: str, filepath: str):
    filepath = str(cwd.joinpath(filepath))

    depositor: file_depositor.S3FileDepositor = file_depositor.S3FileDepositor(
        cwd.joinpath('./crowd_dump.cfg'),
    )
    depositor.deposit_file(s3_bucket_name, s3_folder_name, filepath)

    collector: image_collector.S3ImagesCollector = image_collector.S3ImagesCollector(
        cwd.joinpath('./crowd_dump.cfg'),
    )

    files: pd.DataFrame = collector.get_url_and_name(
        bucket_name=s3_bucket_name,
        folder_name=s3_folder_name,
    )

    assert files.shape[0] == 2

    filename: str = filepath.rsplit('/', maxsplit=1)[-1]

    depositor.delete_file(
        s3_bucket_name=s3_bucket_name,
        s3_filepath=files.loc[0]['Full_path'],
    )

    full_paths: list[str] = sorted(list(files.Full_path.values))

    assert str(pathlib.Path(s3_folder_name).joinpath(filename)) == full_paths[0]
    assert str(pathlib.Path(s3_folder_name).joinpath('test2.txt')) == full_paths[1]
