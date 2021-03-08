import pandas as pd
import kaggle
import os, subprocess
import time
import pickle
import argparse
import glob
import json
import math
from backend.document import Document
from backend.utils import ESHandler, CONFIG

class COVIDChallengeCrawler():

    DATASET_NAME = "allen-institute-for-ai/CORD-19-research-challenge"

    def __init__(self):
        """
            last_fetched (str) : Date of last download in form of "YYYY-MM-DD"
        """ 
        self.data_dir = CONFIG["DATA_DIR"] + "/kaggle"
        self.save_dir = CONFIG["SAVE_DIR"] + "/corpora"

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

        if os.listdir(self.data_dir):
            self.last_fetched = max(os.listdir(self.data_dir))
        else:
            self.last_fetched = ""

        self.parser = COVIDChallengeDocParser()
        self.eshandler = ESHandler()

    def run(self):
        while True:
            print("Start document collection...")
            cmd_output = subprocess.run(
                ["kaggle", "datasets", "list"],
                capture_output=True,
                check=True
            )
            
            stdout = cmd_output.stdout.decode("utf-8").split("\n")
            date = ""
            for line in stdout:
                if COVIDChallengeCrawler.DATASET_NAME in line:
                    info = [item for item in line.split("  ") if item != ""]
                    date = info[3].split()[0]
                    break

            if date == "":
                raise ValueError("Couldn't fetch latest time.")

            if date > self.last_fetched:
                print("Found document to collect...")
                self.last_fetched = date
                self._download_data()
                documents = self._parse_data()
                self._save_data(documents)
                print("Completed collection")

            # Sleep for a day
            print("Sleep")
            time.sleep(86400)
            print("Wake up")

    def _save_data(self, documents):
        timestamp = int(time.time())
        print("Saving collected documents in Elastic Search at:", timestamp)
        file_path = self.save_dir + "/" + str(timestamp) + "_kaggle.corp"
        self.eshandler.insert_many(documents)
        self.eshandler.save()

    def _parse_data(self):
        data_dir = self.data_dir + "/" + self.last_fetched
        self.parser.load_meta_csv((data_dir + "/metadata.csv"))
        json_files = [file for sub_dir in os.walk(data_dir) \
                for file in glob.glob(sub_dir[0] + "/*.json")]

        seen_doi = set()
        documents = []

        for file in json_files:
            doc = self.parser(file)
            if "doi" in doc.metadata and doc.metadata["doi"] in seen_doi:
                continue
            else:
                documents.append(doc)
                if "doi" in doc.metadata:
                    seen_doi.add(doc.metadata["doi"]) 

        return documents

    def _download_data(self):
        # Download data from kaggle
        dest_dir = self.data_dir + "/" + self.last_fetched
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files(
            COVIDChallengeCrawler.DATASET_NAME,
            path=dest_dir, 
            unzip=True
        )

class COVIDChallengeDocParser():

    def __init__(self):
        self.meta_csv = None

    def load_meta_csv(self, meta_csv):
        self.meta_csv = pd.read_csv(meta_csv)

    def _parse_authors(self, authors):
        names = []
        for author in authors:
            name = author["first"] + " " \
                + " ".join(author["middle"]) + " "\
                + author["last"]
            names.append(name)
        return names

    def _parse_text(self, text_list):
        text = ""
        for paragraph in text_list:
            text += paragraph["text"] + "\n"
        return text

    def _format_doi(self, doi):
        # Find the pattern "doi.org/" and remove it if it exists.
        if type(doi) == float:
            print(doi)
        loc = doi.find("doi.org/")
        if loc >= 0:
            return doi[idx+len(tofind):]
        else:
            return doi

    def __call__(self, file_name):
        """
            Take in name of json file to parse and return Document object
        """
        with open(file_name, "r") as fp:
            data = json.load(fp)

            paper_id = data["paper_id"]

            title = data["metadata"]["title"]
            authors = self._parse_authors(data["metadata"]["authors"])
            metadata = {"authors": authors}

            # Look up metadata from metadata.csv
            csv_data = self.meta_csv[self.meta_csv["sha"] == paper_id]
            if not csv_data.empty: 
                doi = csv_data["doi"].iloc[0]
                if not type(doi) is float:
                    doi = self._format_doi(doi)
                    metadata["doi"] = doi
                
                url = csv_data["url"].iloc[0]
                if not type(url) is float:
                    metadata["url"] = url    

            text = {}
            if "abstract" in data:
                abstract = self._parse_text(data["abstract"])
                text["abstract"] = abstract
            if "body_test" in data:
                body_text = self._parse_text(data["body_text"])
                text["body"] = body_text
            if "ref_entries" in data:
                caption_text = self._parse_text(data["ref_entries"].values())
                text["supplementary"] = caption_text

            doc = Document(
                paper_id,
                title,
                metadata,
                text
            )

        return doc

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start crawler")
    parser.add_argument("--target", type=str)
    args = parser.parse_args()

    if args.target.lower() == "kaggle":
        crawler = COVIDChallengeCrawler()
        crawler.run()
