import os
import time
import zipfile
import shutil
import functions


def make_zipfile(file_path, file_name):
    source_dir = file_path + file_name
    output_filename = file_path + 'archving/' + file_name + '.zip'
    if not os.path.exists(file_path + 'archving'):
        os.makedirs(file_path + 'archving')

    with zipfile.ZipFile(output_filename, 'w') as _z:
        _z.write(source_dir, os.path.basename(source_dir),
                 compress_type=zipfile.ZIP_DEFLATED)
        os.remove(source_dir)

    functions.log('############# SUCCESS #############')


def move_archiving(file_path, file_name):
    # mtime = time.strftime("%Y%m%d%H%M%S")
    source_dir = file_path + file_name
    output_filename = file_path + 'archving/' + file_name
    if not os.path.exists(file_path + 'archving'):
        os.makedirs(file_path + 'archving')

    shutil.move(source_dir, output_filename)

    functions.log('############# SUCCESS #############')


def backup_error_file(file_path, file_name):
    source_dir = file_path + file_name
    output_filename = file_path + 'error/' + file_name

    if not os.path.exists(file_path + 'error'):
        os.makedirs(file_path + 'error')

    shutil.move(source_dir, output_filename)

    functions.log('============== FAIL ==============')
