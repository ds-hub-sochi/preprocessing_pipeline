from __future__ import annotations

import asyncio
import os
import pathlib
import shutil

import pandas as pd
import streamlit as st

from crowd_sdk.tagme import ProjectConfig

from src.preprocessing_pipeline.aggregation.box_aggregation import BoxAggregator
from src.preprocessing_pipeline.aggregation import label_aggregation
from src.preprocessing_pipeline.consistency_tests import consistency_tests
from src.preprocessing_pipeline.s3_collectors.image_collector import S3ImagesCollector
from src.preprocessing_pipeline.s3_depositors.file_depositor import S3FileDepositor
from src.preprocessing_pipeline.tagme_collectors.markup_collector import MarkupCollector
from src.preprocessing_pipeline.tagme_creators.tagme_creator import TagmeCreator
from src.preprocessing_pipeline.utils import utils


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
            'Выгрузка разметки из TagMe',
            'Работа с задачами в TagMe',
            'Агрегация разметки',
            'Работа с результатами разметки',
        ],
    )

    crowd_cfg_path: str = str(pathlib.Path.home().joinpath('.crowd.cfg'))

    s3_image_collector: S3ImagesCollector = S3ImagesCollector(crowd_cfg_path)
    s3_file_depositor: S3FileDepositor = S3FileDepositor(crowd_cfg_path)
    markup_collector: MarkupCollector = MarkupCollector(crowd_cfg_path)
    tagme_creator: TagmeCreator = TagmeCreator()

    if 'project_id' not in st.session_state:
        st.session_state.project_id = ''

    if 'organization_id' not in st.session_state:
        st.session_state.organization_id = ''

    if 'person_id' not in st.session_state:
        st.session_state.person_id = ''

    if 'images_dump_dir' not in st.session_state:
        st.session_state.images_dump_dir = ''

    if 'markup_dump_path' not in st.session_state:
        st.session_state.markup_dump_path = ''

    if 'last_task_id' not in st.session_state:
        st.session_state.last_task_id = ''

    if 'aggregation_df' not in st.session_state:
        st.session_state.aggregation_df = ''

    if 'markup_checkpoint_dir' not in st.session_state:
        st.session_state.markup_checkpoint_dir = ''

    if 'aggregation_df_checkpoint' not in st.session_state:
        st.session_state.aggregation_df_checkpoint = ''

    if 'markup_json_dump' not in st.session_state:
        st.session_state.markup_json_dump = ''

    if 'saved_images_dir' not in st.session_state:
        st.session_state.saved_images_dir = ''

    if 'wrong_cases_dir' not in st.session_state:
        st.session_state.wrong_cases_dir = ''

    temp_dir: str = './tmp'
    pathlib.Path.mkdir(pathlib.Path(os.path.abspath(temp_dir)), parents=True, exist_ok=True)

    key: int = 1

    with tabs[0]:
        organization_id: str = st.text_input(
            label='id организации',
            value='',
            key=key,
        )
        st.session_state.organization_id = organization_id
        key += 1

        person_id: str = st.text_input(
            label='Ваш id на TagMe',
            value='',
            key=key,
        )
        st.session_state.person_id = person_id
        key += 1

        operation_type: str = st.selectbox(
            label='Проект:',
            options=(
                'Использовать существующий проект',
                'Создать новый проект',
            ),
            index=None,
            key=key,
        )
        key += 1

        if operation_type == 'Использовать существующий проект':
            project_id: str = st.text_input(
                label='id проекта',
                value='',
                key=key,
            )
            key += 1

            st.session_state.project_id = project_id
        elif operation_type == 'Создать новый проект':
            project_name = st.text_input(
                label='Название проекта',
                value='',
                key=key,
            )
            key += 1

            project_description: str = st.text_input(
                label='Описание проекта',
                help='Необязательное поле',
                key=key,
            )
            key += 1

            labels_str: str = st.text_input(
                label='Перечислите классы, которые есть на фото, через запятую',
                help='Например, "tiger,human,other,empty"',
                key=key,
            )
            key += 1

            labels_lst: list[str] = labels_str.split(',')

            config_files_dir_path: str = st.text_input(
                label='Путь до дирекории с конфигурационными* файлами',
                value='',
                help='HTML, CSS, JS, а также конфиг и инструкция. Названия файлов должны быть, как в примере',
                key=key,
            )
            key += 1

            if all(
                [
                    project_name != '',
                    labels_str != '',
                    config_files_dir_path != '',
                ],
            ):
                if st.button('Создать проект'):
                    config_files_dir_path = os.path.abspath(config_files_dir_path)
                    entities: list[dict[str, str]] = []

                    for label in labels_lst:
                        entities.append(
                            {
                                'id': label,
                                'name': label,
                                'tool': 'bbox',
                                'color': 'blue' if label != 'empty' else 'red',
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

                    pathlib.Path.mkdir('./stub_dir', parents=True, exist_ok=True)

                    _config = ProjectConfig(
                        css=f'{config_files_dir_path}/config_css.css',
                        html=f'{config_files_dir_path}/config_html.html',
                        javascript=f'{config_files_dir_path}/config_js.js',
                        # instruction=f'{config_files_dir_path}/config_instruction.html',
                        instruction=f'{config_files_dir_path}/config_instruction.txt',
                        overlap=1,
                        project_name=project_name,
                        description=project_description,
                        config=__config,
                        data_dir='./stub_dir',
                        example=f'{config_files_dir_path}/config_json.json',
                    )

                    project_id = asyncio.run(
                        tagme_creator.create_project_by_config(
                            organization_id=organization_id,
                            config=_config,
                        ),
                    )
                    st.session_state.project_id = project_id

                    shutil.rmtree('./stub_dir')

                    st.write(f'Создан проект с id {project_id}')
                    st.write('Пожалуйста, назначьте разметчиков:')
                    st.write(f'https://tagme.sberdevices.ru/company/{organization_id}/project/{project_id}/groups')

                    st.write('Пожалуйста, добавьте инструкцию на сайте:')
                    st.write(f'https://tagme.sberdevices.ru/company/{organization_id}/project/{project_id}/brief')

    with tabs[1]:
        bucket: str = st.text_input(
            label='Бакет на S3',
            value='',
            key=key,
        )
        key += 1

        data_dir: str = st.text_input(
            label='Директория на S3 с данными для скачивания',
            value='',
            key=key,
        )
        key += 1

        images_dump_dir: str = st.text_input(
            label='Абсолютный или относительный путь до директории для сохранения',
            value='',
            help='Относительно той директории, откуда запущен скрипт',
            key=key,
        )
        st.session_state.images_dump_dir = images_dump_dir
        key += 1

        if images_dump_dir != '' and data_dir != '' and bucket != '':
            if st.button('Забрать изображения'):
                images_dump_dir = os.path.abspath(images_dump_dir)
                pathlib.Path.mkdir(images_dump_dir, parents=True, exist_ok=True)

                s3_image_collector.get_and_save_images(
                    s3_folder_name=data_dir,
                    s3_bucket_name=bucket,
                    dump_folder_name=images_dump_dir,
                )

    with tabs[2]:
        s3_bucket_name: str = st.text_input(
            label='Бакет на S3',
            value='',
            key=key,
        )
        key += 1

        s3_dump_dir: str = st.text_input(
            label='Директория на S3 для сохранения',
            value='',
            key=key,
        )
        key += 1

        operation_type: str = st.selectbox(
            label='Что нужно отправить',
            options=(
                'Один файл',
                'Всю папку',
            ),
            index=None,
            key=key,
        )
        key += 1

        if operation_type == 'Один файл':
            filepath: str = st.text_input(
                label='Абсолютный или относительный* путь до файла, который нужно отправить на S3',
                help='Относительно той директории, откуда запущен скрипт',
                value='',
                key=key,
            )
            key += 1

            if all(
                [
                    s3_bucket_name != '',
                    s3_dump_dir != '',
                    filepath != '',
                ],
            ):
                if st.button('Отправить'):
                    filepath = os.path.abspath(filepath)

                    s3_file_depositor.deposit_file(
                        s3_bucket_name=s3_bucket_name,
                        s3_folder_name=s3_dump_dir,
                        filepath=filepath,
                    )

                    st.write(f'Файл {filepath} отправлен на S3')
        elif operation_type == 'Всю папку':
            dirpath: str = st.text_input(
                label='Абсолютный или относительный* путь до папки, которую нужно отправить на S3',
                help='Относительно той директории, откуда запущен скрипт',
                value='',
                key=key,
            )
            key += 1

            if all(
                [
                    s3_bucket_name != '',
                    s3_dump_dir != '',
                    dirpath != '',
                ],
            ):
                if st.button('Отправить'):
                    s3_file_depositor.deposite_folder(
                        s3_bucket_name=s3_bucket_name,
                        s3_folder_name=s3_dump_dir,
                        dirpath=dirpath,
                    )

                    st.write('Файлы успешно отправлены')

    with tabs[3]:
        task_id: str = st.text_input(
            label='id задания',
            value=st.session_state.last_task_id,
            key=key,
        )
        key += 1

        st.session_state.last_task_id = task_id

        dump_dir: str = st.text_input(
            label='Абсолютный или относительный* путь до директории для сохранения разметки',
            help='Относительно той директории, откуда запущен скрипт',
            value='',
            key=key,
        )
        key += 1

        if all(
            [
                st.session_state.organization_id != '',
                st.session_state.project_id != '',
                task_id != '',
                dump_dir != '',
            ],
        ):
            if dump_dir is not None:
                dump_dir = os.path.abspath(dump_dir)
                pathlib.Path(dump_dir).mkdir(parents=True, exist_ok=True)

            if st.button('Получить разметку'):
                results: tuple[str, str] = asyncio.run(
                    markup_collector.get_task_markup(
                        organization_id=st.session_state.organization_id,
                        project_id=st.session_state.project_id,
                        task_id=task_id,
                        dump_dir=dump_dir,
                    ),
                )
                st.write(results[0])
                st.session_state.markup_dump_path = str(pathlib.Path(dump_dir).joinpath(results[1]))

    with tabs[4]:
        # task_data = None
        operation_type: str = st.selectbox(
            label='Тип операции',
            options=(
                'Создать задачу',
                'Загрузить даные в задачу',
                'Запустить задачу',
            ),
            index=None,
            key=key,
        )
        key += 1

        if operation_type == 'Создать задачу':
            task_name: str = st.text_input(
                label='Название задания',
                value='',
                key=key,
            )
            key += 1

            overlap: str = st.text_input(
                label='Размер перекрытия',
                value=1,
                help='Должно быть целым положительным числом',
                key=key,
            )
            key += 1

            if all(
                [
                    task_name != '',
                    st.session_state.person_id != '',
                    st.session_state.project_id != '',
                    st.session_state.organization_id != '',
                ],
            ):
                if st.button('Создать задачу'):
                    task_data = asyncio.run(
                        tagme_creator.create_task_in_project(
                            name=task_name,
                            project_id=st.session_state.project_id,
                            organization_id=st.session_state.organization_id,
                            overlap=overlap,
                            person_id=st.session_state.person_id,
                        ),
                    )

                    last_task_id: str = task_data.uid
                    st.write(f'Успешно создана задача с id: {last_task_id}')
                    st.session_state.last_task_id = last_task_id

        elif operation_type == 'Загрузить даные в задачу':
            bucket_name: str = st.text_input(
                label='бакет на S3',
                value='',
                key=key,
            )
            key += 1

            folder_name: str = st.text_input(
                label='Директория с данными на S3',
                value='',
                key=key,
            )
            key += 1

            task_id: str = st.text_input(
                label='Id задачи в TagMe',
                value=st.session_state.last_task_id,
                key=key,
            )
            key += 1

            if all(
                [
                    bucket_name != '',
                    folder_name != '',
                    task_id != '',
                    st.session_state.organization_id != '',
                ],
            ):
                if st.button('Загрузить данные в TagMe'):
                    asyncio.run(
                        utils.upload_s3_to_tagme(
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
                value=st.session_state.last_task_id,
                key=key,
            )
            key += 1

            if all(
                [
                    task_id != '',
                    st.session_state.organization_id != '',
                ],
            ):
                if st.button('Запустить задачу'):
                    asyncio.run(
                        tagme_creator.start_task(
                            task_id=task_id,
                            organization_id=st.session_state.organization_id,
                        ),
                    )
    with tabs[5]:
        path_to_images_dir: str = st.text_input(
            label='Абсолютный или относительный* путь до директории с изображениями для размекти',
            value=os.path.abspath(st.session_state.images_dump_dir),
            help='Относительно той директории, откуда запущен скрипт',
            key=key,
        )
        key += 1
        st.session_state.images_dump_dir = path_to_images_dir

        wrong_cases_dir = st.text_input(
            label='Путь до директории, в которую храняться невалидные случаи',
            value=st.session_state.wrong_cases_dir,
            key=key,
        )
        key += 1
        st.session_state.wrong_cases_dir = wrong_cases_dir

        images_dir_structure: str = st.selectbox(
            label='Структура директории с изображениями',
            options=(
                'nested',
                'flat',
            ),
            help='flat, если структура плоская: F_1_IMG1.JPG; nested, если структура вложенная: F_1/IMG1.JPG;',
            index=None,
            key=key,
        )
        key += 1

        tagme_markup_structure: str = st.selectbox(
            label='Структура разметки',
            options=(
                'nested',
                'flat',
            ),
            help='flat, если структура плоская: F_1_IMG1.JPG; nested, если структура вложенная: F_1/IMG1.JPG;',
            index=None,
            key=key,
        )
        key += 1

        if all(
            [
                path_to_images_dir != '',
                st.session_state.wrong_cases_dir != '',
                images_dir_structure is not None,
                tagme_markup_structure is not None,
            ],
        ):
            operation_type: str = st.selectbox(
                label='Тип операции',
                options=(
                    'Агрегация боксов',
                    'Агрегация лейблов',
                    'Сохранить разметку в формате TagMe',
                ),
                index=None,
                key=key,
            )
            key += 1

            if operation_type == 'Агрегация боксов':
                pathlib.Path.mkdir(
                    pathlib.Path(os.path.abspath(st.session_state.wrong_cases_dir)),
                    parents=True,
                    exist_ok=True,
                )

                box_aggregator: BoxAggregator = BoxAggregator(
                    path_to_images_dir=path_to_images_dir,
                    path_to_wrong_cases_dir=os.path.abspath(st.session_state.wrong_cases_dir),
                    images_dir_structure=images_dir_structure,
                    tagme_markup_structure=tagme_markup_structure,
                )

                bbox_minimal_relative_size: str = st.text_input(
                    label='Минимальная длина одной из сторон бокса в долях от размера изображения',
                    value='0.005',
                    help='Боксы меньше размера будут отброшены. Должно быть действительным число от 0 до 1',
                    key=key,
                )
                key += 1

                bbox_relative_error: str = st.text_input(
                    label='Минимальное в долях от размера изображения расстояние между центрами боксов (bandwidth)',
                    value='0.01',
                    help='Должно быть действительным число от 0 до 1',
                    key=key,
                )
                key += 1

                path_to_markup: str = st.text_input(
                    label='Абсолютный или относительный* путь до файла с разметкой',
                    value=st.session_state.markup_dump_path,
                    help='Относительно той директории, откуда запущен скрипт',
                    key=key,
                )
                key += 1
                st.session_state.markup_dump_path = path_to_markup

                if path_to_markup != '':
                    if st.button('Агрегировать боксы'):
                        aggregation_df: pd.DataFrame = box_aggregator.aggregate(
                            path_to_markup=path_to_markup,
                            bbox_minimal_relative_size=float(bbox_minimal_relative_size),
                            bbox_relative_error=float(bbox_relative_error),
                        )
                        st.write('Агрегация боксов завершена')

                        aggregation_df_dump_path: str = temp_dir + '/aggregation_df.csv'
                        aggregation_df.to_csv(os.path.abspath(aggregation_df_dump_path))

                        # st.session_state.aggregation_df = aggregation_df  # pylint: disable=redefined-variable-type
                        st.session_state.aggregation_df = aggregation_df_dump_path
            elif operation_type == 'Агрегация лейблов':
                aggregator_type: str = st.selectbox(
                    label='Алгоритм агрегации ',
                    options=(
                        'Мнение большинства',
                        'Девид-Скин',
                    ),
                    index=None,
                    key=key,
                )
                key += 1

                if aggregator_type == 'Мнение большинства':
                    label_aggregator: label_aggregation.MajorityVote = label_aggregation.MajorityVote()
                elif aggregator_type == 'Девид-Скин':
                    label_aggregator: label_aggregation.DawidSkene = label_aggregation.DawidSkene()

                checkpoint_dir: str = st.text_input(
                    label='Путь до директории для сохранения результатов агрегации',
                    value='',
                    key=key,
                )
                key += 1

                if checkpoint_dir is not None:
                    st.session_state.markup_checkpoint_dir = os.path.abspath(checkpoint_dir)
                    pathlib.Path(os.path.abspath(checkpoint_dir)).mkdir(parents=True, exist_ok=True)

                if checkpoint_dir != '':
                    if st.button('Агрегировать лейблы'):
                        aggregation_df = pd.read_csv(st.session_state.aggregation_df, index_col=0)

                        aggregated_labels_df: pd.DataFrame = label_aggregator.fit_predict(
                            data=aggregation_df,
                        )
                        st.write('Агрегация лейблов завершена')

                        result_df = aggregation_df.merge(aggregated_labels_df, on=['subtask'])

                        consistent_data: pd.DataFrame = pd.DataFrame(columns=result_df.columns)
                        inconsistent_data: pd.DataFrame = pd.DataFrame(columns=result_df.columns)

                        unique_task_id: list[str] = list(set(result_df['task']))

                        for task_id in unique_task_id:
                            current_task_data: pd.DataFrame = result_df[result_df['task'] == task_id]

                            current_unique_subtask_id = list(set(current_task_data['subtask']))

                            current_subtask_data = current_task_data[
                                current_task_data['subtask'] == current_unique_subtask_id
                            ]

                            consistent_subtask_markup: pd.DataFrame = current_subtask_data[
                                current_subtask_data['label'] == current_subtask_data['aggregated_label']
                            ]

                            inconsistent_subtask_markup: pd.DataFrame = current_subtask_data[
                                current_subtask_data['label'] != current_subtask_data['aggregated_label']
                            ]

                            consistent_data = pd.concat([consistent_data, consistent_subtask_markup])
                            inconsistent_data = pd.concat([inconsistent_data, inconsistent_subtask_markup])

                        inconsistent_data.to_csv(os.path.abspath(wrong_cases_dir) + '/inconsistent_markup.csv')

                        checkpoint: str = (
                            os.path.abspath(st.session_state.markup_checkpoint_dir) + '/consistent_markup.csv'
                        )

                        result_df.to_csv(checkpoint)
                        st.write(f'Разметка в формате csv сохранена в {checkpoint}')

                        st.session_state.aggregation_df_checkpoint = str(checkpoint)
            elif operation_type == 'Сохранить разметку в формате TagMe':
                markup_dump_path: str = st.text_input(
                    label='Путь до разметки из TagMe',
                    value=st.session_state.markup_dump_path,
                    key=key,
                )
                key += 1
                st.session_state.markup_dump_path = markup_dump_path

                aggregated_markup_path: str = st.text_input(
                    label='Путь до агрегированной разметки',
                    value=st.session_state.aggregation_df_checkpoint,
                    key=key,
                )
                key += 1
                st.session_state.aggregation_df_checkpoint = aggregated_markup_path

                dump_dir: str = st.text_input(
                    label='Путь до директории, в которую нужно сохранить результат',
                    value=st.session_state.markup_checkpoint_dir,
                    key=key,
                )
                key += 1
                st.session_state.markup_checkpoint_dir = dump_dir

                if dump_dir != '':
                    pathlib.Path(os.path.abspath(dump_dir)).mkdir(parents=True, exist_ok=True)

                if all(
                    [
                        dump_dir != '',
                        aggregated_markup_path != '',
                        markup_dump_path != '',
                    ],
                ):
                    if st.button('Преобразовать в формат TagMe'):
                        dump_path: str = utils.df_to_tagme_json(
                            os.path.abspath(aggregated_markup_path),
                            os.path.abspath(markup_dump_path),
                            os.path.abspath(dump_dir),
                        )

                        st.write('Разметка преобразована')
                        st.write(f'Разметка в формате TagMe json схранена в {dump_path}')

                        st.session_state.markup_json_dump = dump_path

    with tabs[6]:
        operation_type: str = st.selectbox(
            label='Действия с изображениями',
            options=(
                'Нарисовать разметку и сохранить изображения',
                'Сохранить изображения',
                'Проверка согласованности',
            ),
            index=None,
            key=key,
        )
        key += 1

        path_to_images_dir: str = st.text_input(
            label='Абсолютный или относительный* путь до директории с изображениями для размекти',
            value=st.session_state.images_dump_dir,
            help='Относительно той директории, откуда запущен скрипт',
            key=key,
        )
        key += 1
        st.session_state.images_dump_dir = path_to_images_dir

        images_dir_structure = st.selectbox(
            label='Структура директории с изображениями',
            options=(
                'nested',
                'flat',
            ),
            help='flat, если структура плоская: F_1_IMG1.JPG; nested, если структура вложенная: F_1/IMG1.JPG;',
            index=None,
            key=key,
        )
        key += 1

        tagme_markup_structure = st.selectbox(
            label='Структура разметки',
            options=(
                'nested',
                'flat',
            ),
            help='flat, если структура плоская: F_1_IMG1.JPG; nested, если структура вложенная: F_1/IMG1.JPG;',
            index=None,
            key=key,
        )
        key += 1

        aggregation_result_path = st.text_input(
            label='Путь до файла с агрегированной разметкой',
            value=st.session_state.markup_json_dump,
            key=key,
        )
        key += 1
        st.session_state.markup_json_dump = aggregation_result_path

        if all(
            [
                path_to_images_dir != '',
                images_dir_structure is not None,
                tagme_markup_structure is not None,
                aggregation_result_path != '',
            ],
        ):
            if operation_type == 'Нарисовать разметку и сохранить изображения':
                images_with_markup_dir = st.text_input(
                    label='Путь до директории с размеченными картинками',
                    value='./images_with_markup',
                    key=key,
                )
                key += 1

                if images_with_markup_dir != '':
                    if st.button('Нарисовать разметку'):
                        pathlib.Path(os.path.abspath(images_with_markup_dir)).mkdir(parents=True, exist_ok=True)

                        utils.draw_markup(
                            os.path.abspath(aggregation_result_path),
                            os.path.abspath(path_to_images_dir),
                            os.path.abspath(images_with_markup_dir),
                            images_dir_structure,
                            tagme_markup_structure,
                        )

                        st.write('Изображения с нарисованной разметкой успешно сохранены')
            elif operation_type == 'Сохранить изображения':
                saved_images_dir = st.text_input(
                    label='Путь до директории, в которую нужно сохранить изображения',
                    value='./valid_images',
                    key=key,
                )
                key += 1

                if saved_images_dir != '':
                    if st.button('Сохранить изображения'):
                        pathlib.Path(os.path.abspath(saved_images_dir)).mkdir(parents=True, exist_ok=True)

                        utils.save_valid_images(
                            os.path.abspath(aggregation_result_path),
                            os.path.abspath(path_to_images_dir),
                            os.path.abspath(st.session_state.saved_images_dir),
                            images_dir_structure,
                            tagme_markup_structure,
                        )

                        st.write('Изображения успешно сохранены')
            elif operation_type == 'Проверка согласованности':
                aggregation_result_path = st.text_input(
                    label='Путь до файла с агрегированной разметкой',
                    value=st.session_state.markup_json_dump,
                    key=key,
                )
                key += 1
                st.session_state.markup_json_dump = aggregation_result_path

                saved_images_dir = st.text_input(
                    label='Путь до директории, в которую нужно сохранить изображения',
                    value=st.session_state.saved_images_dir,
                    key=key,
                )
                key += 1
                st.session_state.saved_images_dir = saved_images_dir

                wrong_cases_dir = st.text_input(
                    label='Путь до директории, в которую храняться невалидные случаи',
                    value=st.session_state.wrong_cases_dir,
                    key=key,
                )
                key += 1
                st.session_state.wrong_cases_dir = wrong_cases_dir

                images_dir_structure = st.selectbox(
                    label='Структура директории с изображениями',
                    options=(
                        'nested',
                        'flat',
                    ),
                    help='flat, если структура плоская: F_1_IMG1.JPG; nested, если структура вложенная: F_1/IMG1.JPG;',
                    index=None,
                    key=key,
                )
                key += 1

                tagme_markup_structure = st.selectbox(
                    label='Структура разметкии ',
                    options=(
                        'nested',
                        'flat',
                    ),
                    help='flat, если структура плоская: F_1_IMG1.JPG; nested, если структура вложенная: F_1/IMG1.JPG;',
                    index=None,
                    key=key,
                )
                key += 1

                if all(
                    [
                        aggregation_result_path != '',
                        saved_images_dir != '',
                        st.session_state.wrong_cases_dir != '',
                    ],
                ):
                    if st.button('Проверить согласованность'):
                        st.write(
                            consistency_tests.check_file_to_image(
                                os.path.abspath(aggregation_result_path),
                                os.path.abspath(saved_images_dir),
                                os.path.abspath(st.session_state.wrong_cases_dir),
                                tagme_markup_structure,
                                images_dir_structure,
                            ),
                        )

                        st.write(
                            consistency_tests.check_image_to_file(
                                os.path.abspath(aggregation_result_path),
                                os.path.abspath(saved_images_dir),
                                os.path.abspath(st.session_state.wrong_cases_dir),
                                tagme_markup_structure,
                                images_dir_structure,
                            ),
                        )

                        st.write(
                            consistency_tests.check_unique_field(
                                os.path.abspath(aggregation_result_path),
                                'file_name',
                                os.path.abspath(st.session_state.wrong_cases_dir),
                            ),
                        )

                        st.write(
                            consistency_tests.check_unique_field(
                                os.path.abspath(aggregation_result_path),
                                'item_id',
                                os.path.abspath(st.session_state.wrong_cases_dir),
                            ),
                        )


if __name__ == '__main__':
    main()
