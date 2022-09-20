from deepClassifier.entity import DataIngestionConfig
from deepClassifier import logger
from tqdm import tqdm
import os

import urllib.request as request
from zipfile import ZipFile


class DataIngestion:
    def __init__(self, config: DataIngestionConfig) -> None:
        self.config = config

    def download_file(self):
        logger.info("Trying to download the file")
        file_name = self.config.local_data_file

        if not os.path.exists(file_name):
            logger.info("Download started")
            result_file_name, header= request.urlretrieve(url= self.config.source_URL ,filename=file_name)
            logger.info(f"{file_name} download with following info \n {header}")
        else:
            logger.info(f"{file_name} already exist ")


    def _get_updated_list_of_files(self, list_of_files):
        return [f for f in list_of_files if f.endswith(".jpg") and ("Cat" in f or "Dog" in f)]

    def _preprocess(self, zf: ZipFile, f: str, working_dir: str):
        target_filepath = os.path.join(working_dir, f)
        if not os.path.exists(target_filepath):
            zf.extract(f, working_dir)
        
        if os.path.getsize(target_filepath) == 0:
            logger.info(f"removing file {target_filepath}")
            os.remove(target_filepath)
            
    def unzip_and_clean(self):
        logger.info("unzipping file and removing unwanted files")
        with ZipFile(file=self.config.local_data_file, mode="r") as zf:
            list_of_files = zf.namelist()
            updated_list_of_files = self._get_updated_list_of_files(list_of_files)
            for f in tqdm(updated_list_of_files):
                self._preprocess(zf, f, self.config.unzip_dir)