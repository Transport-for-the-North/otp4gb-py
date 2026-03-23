"""Setup the OTP java environment with maven."""
import os
import pathlib
import zipfile
from urllib.request import urlretrieve
from subprocess import run


BINARY_FOLDER = pathlib.Path("bin")
OSM_NAME = "osmconvert64.exe"
OSMCONVERT_DOWNLOAD_URL = "https://yadi.sk/d/Vnwc4kut3LCBFm"
# TODO(MB) Find the most up to date version of maven by checking https://dlcdn.apache.org/maven/maven-3
MAVEN_VERSION = "3.8.9"
MAVEN_PATH = BINARY_FOLDER / f"apache-maven-{MAVEN_VERSION}/bin/mvn"
MAVEN_DOWNLOAD_URL = (
    f"https://dlcdn.apache.org/maven/maven-3/{MAVEN_VERSION}/"
    f"binaries/apache-maven-{MAVEN_VERSION}-bin.zip"
)


def download_osmconvert():
    """Open OSM download link and check file is moved / named correctly."""
    osm_path = BINARY_FOLDER / OSM_NAME
    if osm_path.is_file():
        return

    print(
        "Download OSM convert with large file support from "
        "https://wiki.openstreetmap.org/wiki/Osmconvert#Binaries"
    )
    # Only open the website once
    os.startfile(OSMCONVERT_DOWNLOAD_URL)

    while not osm_path.is_file():
        print(f'\n{OSM_NAME} not found in "{BINARY_FOLDER.resolve()}"')
        # Cannot download directly from link as it requires clicking
        # the download button so opening link for user to download manually
        input(
            f"""Please download osmconvert64.exe from "{OSMCONVERT_DOWNLOAD_URL}" """
            f"""and place in "{BINARY_FOLDER.resolve()}" with the name "{OSM_NAME}".\n"""
            "Press any key when done..."
        )
        # TODO(MB) Update functionality to automatically click download button
        # urlretrieve(OSMCONVERT_DOWNLOAD_URL, osm_path)


def setup():
    BINARY_FOLDER.mkdir(exist_ok=True)

    download_osmconvert()

    print(f'Downloading maven from "{MAVEN_DOWNLOAD_URL}"')
    maven_zip = BINARY_FOLDER / "maven.zip"
    urlretrieve(MAVEN_DOWNLOAD_URL, maven_zip)
    with zipfile.ZipFile(maven_zip, "r") as zip_ref:
        zip_ref.extractall("bin")

    print("Download complete, running maven to download dependancies")
    run(
        f"{MAVEN_PATH} dependency:copy-dependencies copy-rename:rename",
        shell=True,
        check=True,
    )


if __name__ == "__main__":
    setup()
