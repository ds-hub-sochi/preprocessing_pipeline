from __future__ import annotations

import os

import asyncio
import pathlib
import streamlit as st

from src.preprocessing_pipeline.s3_collectors.image_collector import S3ImagesCollector
from src.preprocessing_pipeline.s3_depositors.file_depositor import S3FileDepositor
from src.preprocessing_pipeline.tagme_collectors.markup_collector import MarkupCollector


def get_style() -> str:
    style = """
                <style>
                .block-container {
                    max-width: 70%
                }
                </style>
             """
    return style


def main():
    st.markdown(get_style(), unsafe_allow_html=True)

    crowd_cfg_path: str = ''

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            'Конфигурация проекта',
            'Выгрузка изображений из S3',
            'Отправление в S3',
            'Забрать разметку из TagMe',
        ],
    )

    with tab1:
        st.header('Конфигурация проекта')

        crowd_cfg_path_candidate: str = st.text_input(
            label='Абсолютный или относительный путь до директории',
        )

        if crowd_cfg_path_candidate != '':
            crowd_cfg_path = os.path.abspath(crowd_cfg_path_candidate)

    with tab2:
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
                label='Локальная директория для хранения скачанных данных',
                value=defaut_input_value,
                help='Относительно той директории, откуда запущен скрипт',
            )

            if dump_dir != defaut_input_value:
                if st.button('Забрать изображения'):
                    dump_dir = os.path.abspath(dump_dir)
                    collector: S3ImagesCollector = S3ImagesCollector(crowd_cfg_path)

                    collector.get_and_save_images(
                        s3_folder_name=data_dir,
                        s3_bucket_name=bucket,
                        dump_folder_name=dump_dir,
                    )
    with tab3:
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

                file_depositor: S3FileDepositor = S3FileDepositor(crowd_cfg_path)
                file_depositor.deposit_file(
                    s3_bucket_name=s3_bucket_name,
                    s3_folder_name=s3_dump_dir,
                    filepath=filepath,
                )

    with tab4:
        organization_id: str = st.text_input(
            label='id орзанизации',
        )

        project_id: str = st.text_input(
            label='id проекта',
        )

        task_id: str = st.text_input(
            label='id задания',
        )

        dump_dir: str = st.text_input(
            label='Абсолютный или относительный* путь до директории для скачивания',
            help='Относительно той директории, откуда запущен скрипт',
        )

        if organization_id != '' and project_id != '' and task_id != '' and dump_dir != '':
            dump_dir = os.path.abspath(dump_dir)
            pathlib.Path(dump_dir).mkdir(parents=True, exist_ok=True)

            if st.button('Получить разметку'):
                murkup_collector: MarkupCollector = MarkupCollector(crowd_cfg_path)

                results_str: str | tuple[str, str] = asyncio.run(
                    murkup_collector.get_task_markup(
                        organization_id=organization_id,
                        project_id=project_id,
                        task_id=task_id,
                        dump_dir=dump_dir,
                    ),
                )

                st.write(results_str)


if __name__ == '__main__':
    main()
