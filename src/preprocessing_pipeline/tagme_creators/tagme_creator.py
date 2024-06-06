from crowd_sdk.tagme import ProjectConfig, TagmeClientAdvanced


class TagmeCreator:
    def __init__(self):
        self._tagme_client: TagmeClientAdvanced = TagmeClientAdvanced()

    async def create_project(
        self,
        organization_id: str,
        project_name: str,
        project_description: str = '',
    ):
        return await self._tagme_client.create_project(
            organization_id=organization_id,
            name=project_name,
            description=project_description,
        )

    async def create_project_by_config(
        self,
        organization_id: str,
        config: ProjectConfig,
    ) -> None:
        return await self._tagme_client.create_project_by_config(
            config=config,
            organization_id=organization_id,
        )

    async def create_task_in_project(
        self,
        project_id: str,
        organization_id: str,
        name: str,
        person_id: str,
        overlap: int = 1,
    ) -> str:
        task_data_ex = {
            'project_id': project_id,
            'organization_id': organization_id,
            'name': name,
            'person_id': person_id,
            'overlap': overlap,
        }

        return await self._tagme_client.create_task(task_data_ex, organization_id)

    async def get_tasks(
        self,
        project_id: str,
        organization_id: str,
    ):
        return await self._tagme_client.get_tasks(
            project_id=project_id,
            organization_id=organization_id,
        )

    async def add_data_to_task(
        self,
        organization_id: str,
        task_id: str,
        file_path: str,
        delimiter: str = ',',
    ) -> None:
        await self._tagme_client.upload_table(
            task_id,
            file_path,
            delimeter=delimiter,
            organization_id=organization_id,
        )

    async def start_task(
        self,
        organization_id: str,
        task_id: str,
    ) -> None:
        await self._tagme_client.start_task(
            task_id=task_id,
            organization_id=organization_id,
        )
