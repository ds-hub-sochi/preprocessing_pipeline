from __future__ import annotations

import asyncio
import os
import pathlib

import streamlit as st

from crowd_sdk.tagme import ProjectConfig

from src.preprocessing_pipeline.s3_collectors.image_collector import S3ImagesCollector
from src.preprocessing_pipeline.s3_depositors.file_depositor import S3FileDepositor
from src.preprocessing_pipeline.tagme_collectors.markup_collector import MarkupCollector
from src.preprocessing_pipeline.tagme_creators.tagme_creator import TagmeCreator
from src.preprocessing_pipeline.utils.utils import upload_s3_to_tagme


def get_style() -> str:
    style = """
                <style>
                .block-container {
                    max-width: 70%
                }
                </style>
             """
    return style


def main():  # pylint: disable=[too-many-locals,too-many-branches,too-many-statements]
    st.markdown(get_style(), unsafe_allow_html=True)

    tabs = st.tabs(
        [
            'Конфигурация проекта',
            'Выгрузка изображений из S3',
            'Отправление в S3',
            'Забрать разметку из TagMe',
            'Создание задачи в TagMe',
        ],
    )

    crowd_cfg_path: str = str(pathlib.Path.home().joinpath('.crowd.cfg'))

    s3_image_collector: S3ImagesCollector = S3ImagesCollector(crowd_cfg_path)
    s3_file_depositor: S3FileDepositor = S3FileDepositor(crowd_cfg_path)
    markup_collector: MarkupCollector = MarkupCollector(crowd_cfg_path)
    tagme_creator: TagmeCreator = TagmeCreator()

    with tabs[0]:
        st.header('Конфигурация проекта')

        if 'project_id' not in st.session_state:
            st.session_state.project_id = None

        if 'organization_id' not in st.session_state:
            st.session_state.organization_id = None

        if 'person_id' not in st.session_state:
            st.session_state.person_id = None

        organization_id: str = st.text_input(
            label='id организации',
            value='',
        )
        st.session_state.organization_id = organization_id

        person_id: str = st.text_input(
            label='Ваш id на TagMe',
            value='',
        )
        st.session_state.person_id = person_id

        operation_type: str = st.selectbox(
            label='Проект:',
            options=(
                'Уже есть',
                'Создать новый',
            ),
            index=None,
        )

        if operation_type == 'Уже есть':
            project_id: str = st.text_input(
                label='id проекта',
                value='',
            )
            st.session_state.project_id = project_id
        elif operation_type == 'Создать новый':
            project_name = st.text_input(
                label='Название проекта',
                value='',
            )

            project_description: str = st.text_input(
                label='Описание проекта',
                help='Необязательное поле',
            )

            labels_str: str = st.text_input(
                label='Перечислите классы, которые есть на фото, через запятую',
                help='Например, "tiger,human,other"',
            )
            labels_lst: list[str] = labels_str.split(',')

            config_files_dir_path: str = st.text_input(
                label='Путь до дирекории с конфигурационными* файлами',
                value='',
                help='HTML, CSS, JS, а также конфиг и инструкция. Названия файлов должны быть, как в примере',
            )

            config_files_dir_path = os.path.abspath(config_files_dir_path)

            overlap: str = st.text_input(label='Укажите размер перекрытия', value=1, help='Должно быть целвым числом')

            if project_name != '' and labels_str != '' and config_files_dir_path != '':
                if st.button('Создать проект'):
                    entities: list[dict[str, str]] = []

                    for label in labels_lst:
                        entities.append(
                            {
                                'id': label,
                                'name': label,
                                'tool': 'bbox',
                            },
                        )

                    entities.append(
                        {
                            'id': 'empty',
                            'name': 'empty',
                            'tool': 'bbox',
                            'color': 'red',
                        },
                    )

                    __config = {
                        'type': 'segmentation',
                        'input_data_category': 'image',
                        'input_schema': {},
                        'output_schema': {},
                        'title': 'Разметка изображений',
                        'shortDescription': 'Выберите класс и выделите часть картинки',
                        'customEntitiesEnabled': False,
                        'groupsEnabled': False,
                        'dataMapping': {'fileUrl': 'image'},
                        'entities': entities,
                    }

                    _config = ProjectConfig(
                        css=f'{config_files_dir_path}/config_css.css',
                        html=f'{config_files_dir_path}/config_html.html',
                        javascript=f'{config_files_dir_path}/config_js.js',
                        instruction=f'{config_files_dir_path}/config_instruction.txt',
                        overlap=int(overlap),
                        project_name=project_name,
                        description=project_description,
                        config=__config,
                        data_dir='./tmp',
                        example=f'{config_files_dir_path}/config_json.json',
                    )

                    project_id = asyncio.run(
                        tagme_creator.create_project_by_config(
                            organization_id=organization_id,
                            config=_config,
                        ),
                    )
                    st.session_state.project_id = project_id

                    st.write(f'Создан проект с id {project_id}')

    with tabs[1]:
        st.header('Выгрузка изображений из S3')

        bucket: str = st.text_input(
            label='Bucket на S3 с данными',
            value='your-bucket-name',
        )

        data_dir: str = st.text_input(
            label='Директория на S3 с данными для скачивания',
            value='your data directory',
        )

        operation_type: str = st.selectbox(
            label='Тип операции',
            options=(
                'Загрузка сразу в Tagme',
                'Локальное сохранение',
            ),
            index=None,
        )

        if operation_type == 'Загрузка сразу в Tagme':
            if st.button('Отправить в TAGME'):
                print('TBA')
        elif operation_type == 'Локальное сохранение':
            defaut_input_value: str = 'Абсолютный или относительный путь до директории'

            dump_dir: str = st.text_input(
                label='путь до локальной директории для хранения скачанных данных',
                value=defaut_input_value,
                help='Относительно той директории, откуда запущен скрипт',
            )

            if dump_dir != defaut_input_value:
                if st.button('Забрать изображения'):
                    dump_dir = os.path.abspath(dump_dir)

                    s3_image_collector.get_and_save_images(
                        s3_folder_name=data_dir,
                        s3_bucket_name=bucket,
                        dump_folder_name=dump_dir,
                    )
    with tabs[2]:
        s3_bucket_name: str = st.text_input(
            label='Имя бакета на S3',
        )

        s3_dump_dir: str = st.text_input(
            label='Директория на S3 для сохранения файла',
        )

        filepath: str = st.text_input(
            label='Абсолютный или относительный* путь до файла',
            help='Относительно той директории, откуда запущен скрипт',
        )

        if s3_bucket_name != '' and s3_dump_dir != '' and filepath != '':
            if st.button('Отправить'):
                filepath = os.path.abspath(filepath)

                s3_file_depositor.deposit_file(
                    s3_bucket_name=s3_bucket_name,
                    s3_folder_name=s3_dump_dir,
                    filepath=filepath,
                )

                st.write(f'Файл {filepath} отправлен на S3')

    with tabs[3]:
        task_id: str = st.text_input(
            label='id задания',
        )

        dump_dir: str = st.text_input(
            label='Абсолютный или относительный* путь до директории для скачивания',
            help='Относительно той директории, откуда запущен скрипт',
        )

        if all(
            [
                st.session_state.organization_id != '',
                st.session_state.project_id != '',
                task_id != '',
                dump_dir != '',
            ],
        ):
            dump_dir = os.path.abspath(dump_dir)
            pathlib.Path(dump_dir).mkdir(parents=True, exist_ok=True)

            if st.button('Получить разметку'):
                results_str: str = asyncio.run(
                    markup_collector.get_task_markup(
                        organization_id=st.session_state.organization_id,
                        project_id=st.session_state.project_id,
                        task_id=task_id,
                        dump_dir=dump_dir,
                    ),
                )

                st.write(results_str)

    with tabs[4]:
        if 'last_task_id' not in st.session_state:
            st.session_state.last_task_id = None

        task_data = None
        operation_type: str = st.selectbox(
            label='Тип операции',
            options=(
                'Создать задачу',
                'Загрузить даные в задачу',
                'Запустить задачу',
            ),
            index=None,
        )

        if operation_type == 'Создать задачу':
            task_name: str = st.text_input(
                label='Название задания',
                value='',
            )

            overlap: str = st.text_input(
                label='Размер перекрытия',
                value=1,
                help='Должно быть целым неотрицательным числом',
            )

            if task_name != '':
                if st.button('Создать задачу'):
                    task_data = asyncio.run(
                        tagme_creator.create_task_in_project(
                            name=task_name,
                            project_id=st.session_state.project_id,
                            organization_id=st.session_state.organization_id,
                            overlap=overlap,
                            person_id=person_id,
                        ),
                    )

                    last_task_id = task_data.uid
                    st.write(f'Успешно создана задача с id: {last_task_id}')
                    st.session_state.last_task_id = last_task_id

        elif operation_type == 'Загрузить даные в задачу':
            bucket_name: str = st.text_input(
                label='бакет на S3',
            )

            folder_name: str = st.text_input(
                label='Папка с данными в бакете',
            )

            task_id: str = st.text_input(
                label='Id задачи в TagMe',
                value=st.session_state.last_task_id if st.session_state.last_task_id is not None else '',
            )

            if st.button('Загрузить данные в TagMe'):
                asyncio.run(
                    upload_s3_to_tagme(
                        bucket_name,
                        folder_name,
                        task_id,
                        st.session_state.organization_id,
                    ),
                )
                st.write('Данные успешно отправлены в TagMe')
        elif operation_type == 'Запустить задачу':
            task_id: str = st.text_input(
                label='Id задачи в TagMe',
                value=st.session_state.last_task_id if st.session_state.last_task_id is not None else '',
            )

            if task_id != '':
                if st.button('Запустить задачу'):
                    asyncio.run(
                        tagme_creator.start_task(
                            task_id=task_id,
                            organization_id=st.session_state.organization_id,
                        ),
                    )


if __name__ == '__main__':
    main()
