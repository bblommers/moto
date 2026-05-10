import io
import os
import shutil
import tarfile
import zipfile
from tempfile import TemporaryDirectory

import pytest

from moto.awslambda.models import zip2tar


@pytest.mark.parametrize("permissions", [0o400, 0o550, 0o755])
def test_zip2tar_keeps_permissions(permissions):
    with TemporaryDirectory() as temp_dir:
        # File it
        file_name = os.path.join(temp_dir, "temp.txt")
        with open(file_name, mode="w") as f:
            f.write(f"some data as {permissions}")
        os.chmod(file_name, permissions)

        # Zip it
        zip_output = io.BytesIO()
        zip_file = zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED)
        zip_file.write(file_name, arcname="fileinzip.txt")
        zip_file.close()
        zip_output.seek(0)

        # Tar it
        tar_bytes = zip2tar(zip_output.read())
        tar_file = tarfile.TarFile.open(fileobj=tar_bytes)

        ## CHECK 1
        # Tar object should have the supplied permissions
        tarred_file = tar_file.getmember("fileinzip.txt")
        assert tarred_file.mode == permissions

        # Close it
        tar_file.close()

        # Full-circle it
        tar_bytes.seek(0)
        tar_name = os.path.join(temp_dir, "temp.txt.tar")
        with open(tar_name, mode="wb") as f:
            f.write(tar_bytes.read())

        untarred_folder = os.path.join(temp_dir, "untarred")
        shutil.unpack_archive(filename=tar_name, extract_dir=untarred_folder)

        status = os.stat(os.path.join(untarred_folder, "fileinzip.txt"))

        ## CHECK 2
        # Extracted file should have the same permissions
        # Note that we're receiving the file type AND file permission here, i.e. 0o100744
        # We're only interested in the file permission (0o744)
        assert status.st_mode & 0o777 == permissions
