import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s: %(message)s]')

package_nem = "deepClassifier"

list_of_files =[
    ".github/workflows/.gitkeep",
    f"src/{package_nem}/components/__init__.py",
    f"src/{package_nem}/utils/__init__.py",
    f"src/{package_nem}/config/__init__.py",
    f"src/{package_nem}/pipeline/__init__.py",
    f"src/{package_nem}/entity/__init__.py",
    f"src/{package_nem}/constants/__init__.py",
    f"configs/config.yaml",
    "dvc.yaml",
    "parameter.yaml",
    "init_setup.sh",
    "requirements.txt",
    "requirements_dev.txt",
    "setup.py",
    "setup.cfg",
    "pyproject.toml",
    "tox.ini",
    "research/trials.ipynb",    
]

for filepath in list_of_files:
    filepath = Path(filepath)
    filedir, filename = os.path.split(filepath)
    if filedir != "":
        os.makedirs(filedir, exist_ok=True)
        logging.info(f"creating directoy: {filedir} for file {filename}")

        if (not os.path.exists(filepath)) or (os.path.getsize(filepath) == 0):
            with open(filepath, "w") as f:
                pass
                logging.info(f"creating directory : {filedir}")
        else:
            logging.info("file already exist : {filename}")