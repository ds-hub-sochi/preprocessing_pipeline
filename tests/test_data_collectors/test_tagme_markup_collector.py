import asyncio
import os
import pathlib
import shutil

import pytest

from src.preprocessing_pipeline.tagme_collectors import markup_collector

cwd: pathlib.Path = pathlib.Path(__file__).parent.resolve()


test_data = (
    (
        '8f9375d6-4f3a-48d5-9a03-ac4e3977cb63',
        '2669e20f-624a-41f6-907a-c6d96db75e17',
        '6029d051-7829-4d8f-a3c4-33cd3bfa6984',
    ),
)


@pytest.mark.parametrize('organization_id,project_id,task_id', test_data)
def test_get_markup_from_finished_task(organization_id: str, project_id: str, task_id: str):
    dump_dir: str = './dump'
    dump_dir = os.path.abspath(dump_dir)
    pathlib.Path(dump_dir).mkdir(parents=True, exist_ok=True)

    murkup_collector: markup_collector.MarkupCollector = markup_collector.MarkupCollector(
        cwd.joinpath('crowd_dump.cfg'),
    )

    result_str: str = asyncio.run(
        murkup_collector.get_task_markup(
            organization_id=organization_id,
            project_id=project_id,
            task_id=task_id,
            dump_dir=dump_dir,
        ),
    )

    assert result_str == f'Разметка сохранена в {pathlib.Path(dump_dir).joinpath("Детекция_тигров_part15.json")}'
    assert os.path.isfile(f'{pathlib.Path(dump_dir).joinpath("Детекция_тигров_part15.json")}')

    shutil.rmtree(dump_dir)


test_data = (('8f9375d6-4f3a-48d5-9a03-ac4e3977cb63', '2669e20f-624a-41f6-907a-c6d96db75e17'),)


@pytest.mark.parametrize('organization_id,project_id', test_data)
def test_get_markup_by_wrong_task_id(organization_id: str, project_id: str):
    dump_dir: str = './dump'
    dump_dir = os.path.abspath(dump_dir)
    pathlib.Path(dump_dir).mkdir(parents=True, exist_ok=True)

    murkup_collector: markup_collector.MarkupCollector = markup_collector.MarkupCollector(
        cwd.joinpath('crowd_dump.cfg'),
    )

    result_str: str = asyncio.run(
        murkup_collector.get_task_markup(
            organization_id=organization_id,
            project_id=project_id,
            task_id=1,
            dump_dir=dump_dir,
        ),
    )

    assert result_str == 'Не получилось найти задание с таким id'
    assert not os.path.isfile(f'{pathlib.Path(dump_dir).joinpath("*.json")}')

    shutil.rmtree(dump_dir)
