import os
import shutil


def get_waiting_data_paths(data_path: str, domain: str):
    data_path = os.path.join(data_path, 'waiting')

    # find paths of all directories which contains waiting data
    data_paths = [os.path.join(data_path, part, domain) for part in os.listdir(data_path)]

    for path in data_paths:
        if not os.path.exists(path):
            continue

        for file_name in os.listdir(path):
            res = os.path.join(path, file_name)
            if os.path.exists(res):
                yield res


def move_complete_data(file_path: str) -> str:
    splited_path = file_path.split('/')
    indx = splited_path.index('waiting')
    splited_path[indx] = 'complete'
    destination_path = '/' + '/'.join(splited_path[1:-1])

    if not os.path.exists(destination_path):
        os.mkdir(destination_path)

    return shutil.move(file_path, destination_path)
