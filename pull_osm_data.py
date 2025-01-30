# Copyright (c) 2025 <Godwin peter. O>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#  Project: python-experiments
#  Author: Godwin peter. O (me@godwin.dev)
#  Created At: Wed 29 Jan 2025 17:05:45
#  Modified By: Godwin peter. O (me@godwin.dev)
#  Modified At: Wed 29 Jan 2025 17:05:45

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import boto3
import osmium
import requests

# NOTE: Install these first
# Resolve any virtual env issue
# pip install --upgrade boto3 osmium

## Valid ENV Variables - .osm.env OR .env
# DATA_DIR=./data
# OSM_LOCATIONS=africa/mali,africa/togo
# OSM_SOURCE=https://<source-url>
# S3_ENABLED=False
# S3_ACCOUNT_ID=xx
# S3_ENDPOINT_URL=xx
# S3_BUCKET_NAME=xx
# S3_ACCESS_KEY_ID=xx
# S3_ACCESS_KEY_SECRET=xx


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_env_file(input_env_file, override=False):
    dotenv_path: str = ".env"
    file_path = Path(input_env_file)
    if file_path.is_file():
        dotenv_path = input_env_file
    elif Path(dotenv_path).is_file():
        pass
    else:
        logger.critical("Unable to load env vars")
        sys.exit()

    with open(dotenv_path) as file_obj:
        lines = file_obj.read().splitlines()  # Removes \n from lines

    dotenv_vars = {}
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", maxsplit=1)
        dotenv_vars.setdefault(key, value)

    if override:
        os.environ.update(dotenv_vars)
    else:
        for key, value in dotenv_vars.items():
            os.environ.setdefault(key, value)


load_env_file(".osm.env")

output_name = "all"
locations_conf = os.getenv("OSM_LOCATIONS", "africa/togo,africa/benin")
logger.info(locations_conf)
all_locations: List[str] = locations_conf.split(",")

DATA_DIR = os.getenv("DATA_DIR", "./")
DOWNLOAD_DIR = f"{DATA_DIR}/osm_downloads"
OSM_DIR = f"{DATA_DIR}/openstreetmap"
OSM_SOURCE = os.getenv("OSM_SOURCE")
S3_ENABLED: bool = eval(os.getenv("S3_ENABLED", "False"))
## account_id - For cloudflare OR replace with S3 url
S3_ACCOUNT_ID = os.getenv("S3_ACCOUNT_ID")
S3_ENDPOINT_URL = os.getenv(
    "S3_ENDPOINT_URL", f"https://{S3_ACCOUNT_ID}.r2.cloudflarestorage.com"
)
S3_CONFIG = {
    "bucket_name": os.getenv("S3_BUCKET_NAME", ""),
    "endpoint_url": S3_ENDPOINT_URL,
    "access_key_id": os.getenv("S3_ACCESS_KEY_ID", ""),
    "access_key_secret": os.getenv("S3_ACCESS_KEY_SECRET", ""),
}


class OSMDownloaderMerger:
    def __init__(self, base_url: str = "", locations: List[str] = []):
        self.base_url = base_url
        self.locations = locations

        try:
            if not os.path.exists(OSM_DIR):
                os.makedirs(OSM_DIR)

            if not os.path.exists(DOWNLOAD_DIR):
                os.makedirs(DOWNLOAD_DIR)
        except FileExistsError:
            pass

    def needs_download(self, filepath: str) -> bool:
        """Check if file needs to be downloaded based on age or existence."""
        if not os.path.exists(filepath):
            return True

        file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        age = datetime.now() - file_time
        return age.days >= 7

    def clean_old_merge(self, file_input: str):
        logger.info("Cleaning up old merged file..")
        file_path = Path(file_input)
        try:
            file_path.unlink()
            logger.info(f"{file_path} has been deleted successfully.")
        except FileNotFoundError:
            logger.error(f"{file_path} does not exist.")
        except PermissionError:
            logger.error(f"Permission denied: {file_path}.")
        except Exception as e:
            logger.error(f"Error occurred: {e}")

    def download_files(self) -> List[str]:
        """Download OSM files if needed."""
        downloaded_files = []

        for loc in self.locations:
            paths = loc.split("/")
            country = paths[1]
            filename = f"{DOWNLOAD_DIR}/{country}-latest.osm.pbf"
            filepath = f"{loc}-latest.osm.pbf"

            if self.needs_download(filename):
                url = f"{self.base_url}/{filepath}"
                logger.info(f"Downloading OSM file for {loc}")

                try:
                    response = requests.get(url, stream=True)
                    response.raise_for_status()

                    with open(filename, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    logger.info(f"Successfully downloaded {loc} OSM data")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Failed to download {loc} OSM data: {str(e)}")
                    continue

            downloaded_files.append(filename)

        return downloaded_files

    def merge_files(
        self, input_files: List[str], output_file: str = "all.osm.pbf"
    ) -> Optional[str]:
        """Merge downloaded OSM files using osmium."""
        try:
            logger.info("Starting merge process")

            self.clean_old_merge(output_file)
            handler = osmium.SimpleWriter(output_file)
            reader = osmium.MergeInputReader()

            for input_file in input_files:
                logger.info(f"Processing {input_file}")

                reader.add_file(input_file)

            reader.apply(handler)

            logger.info(f"Successfully merged files into {output_file}")

            return output_file

        except Exception as e:
            logger.error(f"Failed to merge files: {str(e)}")
            return None

    def upload_to_r2(
        self,
        file_path: str,
        bucket_name: str,
        endpoint_url: str,
        access_key_id: str,
        access_key_secret: str,
    ) -> bool:
        """Upload merged file to Cloudflare R2."""
        try:
            logger.info("Initiating upload to Cloudflare R2")

            # Configure R2 client
            s3_client = boto3.client(
                service_name="s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=access_key_secret,
            )

            # Upload file
            with open(file_path, "rb") as file_data:
                s3_client.upload_fileobj(
                    file_data, bucket_name, os.path.basename(file_path)
                )

            logger.info(f"Successfully uploaded {file_path} to R2")
            return True

        except Exception as e:
            logger.error(f"Failed to upload to R2: {str(e)}")
            return False


def main():
    if OSM_SOURCE is None:
        logger.critical("Incorrect download URL")
        sys.exit()
    else:
        osm_handler = OSMDownloaderMerger(OSM_SOURCE, all_locations)
        downloaded_files: List[str] = osm_handler.download_files()

        if not downloaded_files:
            logger.error("No files were downloaded or found locally")
            return

        merged_file: str | None = osm_handler.merge_files(
            downloaded_files, f"{OSM_DIR}/{output_name}.osm.pbf"
        )
        if not merged_file:
            logger.error("Failed to merge files")
            return

        # Upload to R2
        if S3_ENABLED:
            upload_success = osm_handler.upload_to_r2(merged_file, **S3_CONFIG)
            if upload_success:
                logger.info("Process completed successfully")
            else:
                logger.error("Process completed with errors")
        else:
            logger.info(f"Upload to {S3_CONFIG['endpoint_url']} is disabled...")


if __name__ == "__main__":
    main()
