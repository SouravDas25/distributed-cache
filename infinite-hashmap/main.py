import os
import pathlib
import shutil
import datetime
import pandas as pd
import filedate
import json


def changeFileCreationTime(file_path, creation_datetime):
    # creation_datetime = datetime.datetime(2022, 1, 1, 10, 30)
    # modification_datetime = datetime.datetime(2022, 1, 1, 11, 0)
    # creation_time = creation_datetime.timestamp()
    file_path = pathlib.Path(file_path)
    a_file = filedate.File(file_path)

    a_file.set(
        created=creation_datetime,
        modified=creation_datetime,
        accessed=creation_datetime
    )
    print(a_file.get())
    # file_path.stat().st_ctime = creation_datetime.timestamp()
    # file_path.stat().st_mtime = creation_datetime.timestamp()


def is_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            json.load(file)
        return True
    except:
        return False


def get_all_files(directory):
    all_files = []

    # Walk through all directories and subdirectories
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if is_json_file(file_path):
                with open(file_path, 'r') as file:
                    metadata = json.load(file)
                if "photoTakenTime" in metadata and "title" in metadata:
                    timestamp = metadata["photoTakenTime"]["timestamp"]
                    creation_datetime = datetime.datetime.fromtimestamp(int(timestamp))
                    title = metadata["title"]
                    setFilePath = os.path.join(os.path.dirname(file_path), title)
                    if not os.path.exists(setFilePath):
                        continue
                    print("setFilePath", setFilePath)
                    print("creation_datetime", creation_datetime)
                    confrm = "Y"
                    # confrm = input("Continue [Y/N] : ")
                    if confrm == "Y":
                        changeFileCreationTime(setFilePath, creation_datetime)
                all_files.append(file_path)

    return all_files


def main():
    allFiles = get_all_files("Z:\Sourav\MyPhoto\Takeout")
    with open("all-files.json", 'w') as json_file:
        json.dump(allFiles, json_file, indent=4)


if __name__ == '__main__':
    main()
