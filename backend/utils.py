import os, sys
import json
import time
from os.path import dirname as parent
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError
from elasticsearch.helpers import scan
try:
    from document import Document
except:
    from .document import Document

project_path = parent(parent(os.path.realpath(__file__)))
config_path = project_path + "/config.json"

with open(config_path) as fp:
    CONFIG = json.load(fp)

CONFIG["DATA_DIR"] = project_path + "/" + CONFIG["DATA_DIR"]
CONFIG["SAVE_DIR"] = project_path + "/" + CONFIG["SAVE_DIR"]


def pip_install(url):
    # Used to download Spacy models
    subprocess.check_call([sys.executable, "-m", "pip", "install", url])

class ESHandler():
    """
    Class used to handle communication with Elastic Search
    """
    def __init__(self):
        self.client = Elasticsearch(CONFIG["ES_HOST"])
        self.index = CONFIG["ES_INDEX"]
        self.snapshot_path = CONFIG["SAVE_DIR"] + "/elasticsearch/" + self.index

        try:
            self.client.snapshot.verify_repository(
                repository=self.index
            )
        except NotFoundError:
            self.client.snapshot.create_repository(
                repository=self.index,
                body={"type": "fs",
                    "settings": {
                        "location": self.index
                    }
                }
            )

    def search(self, query, size=100):
        try:
            body = { "query": { 
                "query_string" : {
                    "query" : query,
                    "fields" : ["title^3", "content.abstract^2", "content.body^1", "content.supplementary^1"]
                }
            }}
            resp = self.client.search(
                body=body,
                index=self.index,
                size=size
            )
            return resp
        except NotFoundError:
            return None

    def advanced_search(self, query_body, size=100):
        try:
            resp = self.client.search(
                body=query_body,
                index=self.index,
                size=size
            )
            return resp
        except NotFoundError:
            return None

    def get(self, id):
        try:
            resp = self.client.get(index=self.index, id=id)
            doc = resp["_source"]
            return Document.from_dict(doc)
        except NotFoundError:
            return None

    def get_many(self, ids):
        query = json.dumps({"docs": [{"_id": id} for id in ids]})
        resp = self.client.mget(body=query, index=self.index)
        results = resp["docs"]
        documents = []
        for result in results:
            if result["found"]:
                doc = result["_source"]
                documents.append(Document.from_dict(doc))
            else:
                documents.append(None)
        return documents

    def get_all_ids(self):
        response = scan(
            self.client,
            index=self.index,
            query={"query": { "match_all" : {}}, "stored_fields": []}
        )
        ids = [item["_id"] for item in response]
        return ids

    def get_all_docs(self):
        response = scan(
            self.client,
            index=self.index,
            query={"query": { "match_all" : {}}}
        )
        documents = [Document.from_dict(item["_source"]) for item in response]
        return documents

    def insert(self, doc):
        json_doc = doc.to_json()
        try:
            resp = self.client.index(
                index=self.index,
                body=json_doc,
                id=doc.id
            )
        except elasticsearch.exceptions.RequestError as err:
            print(err.info)

        if resp["result"] != "created" and resp["result"] != "updated":
            raise Exception("Failed to insert document!")

    def insert_many(self, documents):
        for doc in documents:
            self.insert(doc)

    def save(self):
        self.t = str(int(time.time()))
        self.client.snapshot.create(
            repository=self.index,
            snapshot=self.t
        )

    def restore(self):
        # Find latest snapshot name
        info = ""
        for file in os.listdir(self.snapshot_path):
            if "index-" in file:
                info = self.snapshot_path + "/" + file
        if info == "":
            raise Exception("File for snapshot name not found.")
        with open(info, "r") as fp:
            info = json.load(fp)
        snapshots = [s["name"] for s in info["snapshots"]]
        latest = max(snapshots)

        self.client.snapshot.restore(
            repository=self.index,
            snapshot=latest
        )

