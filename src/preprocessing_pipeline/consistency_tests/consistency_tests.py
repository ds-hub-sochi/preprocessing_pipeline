import glob
import json
import os


def check_file_to_image(
    markup_file_path: str,
    images_dir: str,
    wrong_cases_dir: str,
    markup_file_structure: str,
    images_dir_structure: str,
):
    wrong_cases: list[str] = []
    with open(markup_file_path, 'r', encoding='utf-8') as markup_file:
        markup = json.load(markup_file)

    for sample in markup:
        file_name: str = sample['file_name']

        if markup_file_structure != images_dir_structure:
            if markup_file_structure == 'nested':
                file_name = file_name.replace('/', '_')
            elif markup_file_structure == 'flat':
                file_name = file_name.replace('_', '/')

        if not os.path.exists(f'{images_dir}/{file_name}'):
            wrong_cases.append(file_name)

    if len(wrong_cases) == 0:
        return 'Для каждого изображения в разметке существует реальное изображение'

    with open(f'{wrong_cases_dir}/file_to_image.txt', 'w', encoding='utf-8') as bad_cases_file:
        for bad_case in wrong_cases:
            bad_cases_file.write(bad_case + '\n')

    return f'Для {len(wrong_cases)} изображений нет реального изображения. Смотри {wrong_cases_dir}/file_to_image.txt'


def check_image_to_file(
    markup_file_path: str,
    images_dir: str,
    wrong_cases_dir: str,
    markup_file_structure: str,
    images_dir_structure: str,
):
    wrong_cases: list[str] = []
    with open(markup_file_path, 'r', encoding='utf-8') as markup_file:
        markup = json.load(markup_file)

    files = glob.glob(f'{images_dir}/**/*.JPG', recursive=True)

    for file in files:
        file: str = file.replace('\\', '/')
        file_name: str = '/'.join(file.split('/')[-1:])

        if markup_file_structure != images_dir_structure:
            if markup_file_structure == 'nested':
                file_name = file_name.replace('/', '_')
            elif markup_file_structure == 'flat':
                file_name = file_name.replace('_', '/')

        print(file_name)

        found = False
        for sample in markup:
            if sample['file_name'] == file_name:
                found = True
                break
        if not found:
            wrong_cases.append(file_name)

    if len(wrong_cases) == 0:
        return 'Для каждого валидного файла найдена разметка'

    with open(f'{wrong_cases_dir}/image_to_file.txt', 'w', encoding='utf-8') as bad_cases_file:
        for bad_case in wrong_cases:
            bad_cases_file.write(bad_case + '\n')

    return f'Для {len(wrong_cases)} валидных изображений нет разметки. Смотри {wrong_cases_dir}/image_to_file.txt'


def check_unique_field(
    markup_file_path: str,
    field: str,
    wrong_cases_dir: str,
):
    wrong_cases: list[str] = []
    unique_values = set()

    with open(markup_file_path, 'r', encoding='utf-8') as markup_file:
        markup = json.load(markup_file)

    for sample in markup:
        if sample[field] not in unique_values:
            unique_values.add(sample[field])
        else:
            wrong_cases.append(sample[field])

    if len(wrong_cases) == 0:
        return f'Все значения {field} уникальны'

    with open(f'{wrong_cases_dir}/not_unique_{field}.txt', 'w', encoding='utf-8') as bad_cases_file:
        for bad_case in wrong_cases:
            bad_cases_file.write(bad_case + '\n')

    return f'Для {len(wrong_cases)} валидных изображений нет разметки. Смотри {wrong_cases_dir}/not_unique_{field}.txt'
