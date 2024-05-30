from __future__ import annotations

import pathlib

from crowd_sdk.tagme import TagmeClientAdvanced
from crowd_sdk.tagme.cli.download_task_results import download_task_table
from crowd_sdk.tagme.http_client.datacls import TaskData


class MarkupCollector:
    def __init__(self, config_path: str):
        self._tagme_client: TagmeClientAdvanced = TagmeClientAdvanced(config_path)

    def _get_task(self, task_id: str, tasks: list[TaskData]) -> TaskData:
        for task in tasks:
            if task.uid == task_id:
                return task

        return None

    async def get_task_markup(
        self,
        organization_id: str,
        project_id: str,
        task_id: str,
        dump_dir: str,
    ) -> str:
        await self._tagme_client.set_organization(organization_id)
        tasks = await self._tagme_client.get_tasks(project_id=project_id)

        got_current_task: bool = False

        for task in tasks:
            if task.uid == task_id:
                got_current_task = True
                break

        result_str: str = ''

        if not got_current_task:
            result_str = 'Не получилось найти задание с таким id'

            return result_str

        task = self._get_task(task_id, tasks)

        task_stats = await self._tagme_client.get_task_stats(task_id, organization_id)

        marked_count: int = task_stats.marked_count
        objects_count: int = task_stats.objects_count

        if task_stats.objects_count != task_stats.marked_count:
            result_str += f'Разметка не завершена: размечено {marked_count} из {objects_count} объектов.'
        else:
            result_str += 'Разметка завершена.'

        result_str += '\n\n'

        dump_file_path: pathlib.Path = pathlib.Path(dump_dir).joinpath(task.name.replace(' ', '_') + '.json')

        _ = await download_task_table(
            self._tagme_client,
            task_id=task.uid,
            destination=dump_file_path,
            ext='json',
            join_markers=True,
            date=None,
            group_markers=False,
            close=True,
        )

        result_str += f'Разметка сохранена в {dump_file_path}'

        return result_str
