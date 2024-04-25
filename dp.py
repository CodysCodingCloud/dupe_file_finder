import os
import hashlib
import json
import asyncio
import concurrent.futures
import threading
import time
import sys
from datetime import datetime

async def main(file_extension, directory):
    hashed = 0
    while not file_extension:
        print("Please provide an extention.")
        file_extension = input("input an extention")

    hash_file = f"hashed_files.{file_extension}.json"
    file_list = f"file_list.{file_extension}.json"
    dupe_hash_file = f"dupe_hash_file.{file_extension}.json"
    dir_list = f"dir_list.json"
    # file_hash_dict = {}
    hash_file_dict = {}

    print(f"Searching *.{file_extension}...")
    mylist = [
        os.path.join(root, file)
        for root, dirs, files in os.walk(directory)
        for file in files
        if file.endswith(f".{file_extension}")
    ]

    with open(file_list, "w", encoding="utf-8") as file:
        json.dump(mylist, file, ensure_ascii=False, indent=2)
    print(f"found {len(mylist)} {file_extension} files")

    dir_list = f"dir_list.json"
    my_dir_list = [root for root, dirs, files in os.walk(directory)]
    with open(dir_list, "w", encoding="utf-8") as file:
        json.dump(my_dir_list, file, ensure_ascii=False, indent=2)
    print(f"found {len(my_dir_list)} folders")

    # Check if the old hash file exists
    if os.path.isfile(hash_file):
        print("Loading hashed list from file...")
        with open(hash_file, "r", encoding="utf-8") as file:
            old_data = json.load(file)
        print(f"old list contained {len(old_data)} files")
        file_to_hash_dict = {}
        for file_hash, dupe_list in old_data.items():
            for file_name in dupe_list:
                file_to_hash_dict[file_name] = file_hash

        # filenum=0

        # Use an executor to run the CPU-intensive task in a separate thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            new_hash_list = []
            ordernum = 0
            for file_path in mylist:
                ordernum += 1
                hash_value = file_to_hash_dict.get(file_path)
                if not hash_value:
                    hashed += 1
                    print(f"hash DNE {ordernum}:  {file_path}")
                    future_res = executor.submit(hashfile, file_path)
                    hash_value = future_res.result()
                else:
                    print(f"hash found{ordernum}:{file_path}")

                new_hash_list.append(hash_value)
        for file_path, hash_value in zip(mylist, new_hash_list):
            update_dict_with_list(hash_file_dict, hash_value, file_path)
    else:
        print("first time user, Hashing...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            result_hash_list = executor.map(hashfile, mylist)
        hashed += len(mylist)
        for file_path, hash_value in zip(mylist, result_hash_list):
            update_dict_with_list(hash_file_dict, hash_value, file_path)

    print(f"saving hash dict to {hash_file}")
    with open(hash_file, "w", encoding="utf-8") as file:
        json.dump(hash_file_dict, file, ensure_ascii=False, indent=2)

    print(f"hashed {hashed} files")

    dupe_dict = {}
    num_dupe_hashes = 0
    num_dupe_files = 0
    dupe_counter = {}
    for hash_value, file_list in hash_file_dict.items():
        len_of_files_list = len(file_list)
        dupe_counter.setdefault(len_of_files_list, 0)
        dupe_counter[len_of_files_list] += 1
        if len_of_files_list > 1:
            num_dupe_hashes += 1
            num_dupe_files += len_of_files_list
            for x in range(len(file_list)):
                file_path=file_list[x]
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                file_list[x] = f"{mod_time} {file_path}"
            dupe_dict[hash_value] = file_list
    # save dupes in dict format
    with open(dupe_hash_file, "w", encoding="utf-8") as file:
        # file.write(json.dumps(hash_file_dict))
        json.dump(dupe_dict, file, ensure_ascii=False, indent=2)
    print(f"dupes: {num_dupe_hashes}\nfiles: {num_dupe_files}\ncounts: {dupe_counter}")


def hashfile(filepath):
    with open(filepath, "rb") as f:
        hash_object = hashlib.md5()
        hash_object.update(f.read())
        hash_value = hash_object.hexdigest()
    return hash_value


def update_dict_with_list(existing_dict: dict, key: str, item: str):
    existing_dict.setdefault(key, []).append(item)


if __name__ == "__main__":
    # ext = input('input an extention: ')
    if len(sys.argv) > 1:
        ext = sys.argv[1]
    else:
        ext = "jpg"
    directory = "."
    t1 = time.time()
    asyncio.run(main(ext, directory))
    t2 = time.time()
    elapsed_time_seconds = t2 - t1
    hours = int(elapsed_time_seconds // 3600)
    minutes = int((elapsed_time_seconds % 3600) // 60)
    seconds = int(elapsed_time_seconds % 60)
    print(f"Elapsed time: {hours} : {minutes} : {seconds}")
