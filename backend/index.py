from gensim import corpora, models, similarities
import config
import pickle
import heapq
import os, time
import multiprocessing as mp
from .utils import ESHandler, CONFIG
from .document import Document
from .ner import Metamap

class Index():

    def init(self, *kwargs):
        """ Freshly create the index """
        raise NotImplementedError

    def update(self):
        """ Check if we need to update the index and do update if needed"""
        raise NotImplementedError

    def query(self, query):
        """  Take query and return list of tuples of the form (document, score) """
        raise NotImplementedError

    def save(self):
        """ Save itself"""
        raise NotImplementedError

    @staticmethod
    def load_latest():
        """ Load the latest stored index """
        raise NotImplementedError

class ElasticSearchIndex(Index):

    def __init__(self):
        self.es_handler = ESHandler()
        self.metamap = Metamap()

    def init(self):
        pass

    def update(self):
        pass
    
    def query(self, query):
        concepts = self.metamap(query)
        for concept in concepts:
            query += " OR (content.concepts:" + concept + ")"

        print("query: ", query)
        query_body = { 
            "query": {
                "query_string" : {
                    "query" : query,
                    "fields" : [
                        "title^3",
                        "content.abstract^2",
                        "content.body",
                        "content.supplementary",
                        "content.concepts^4"
                    ]
                }
            },
            "highlight" : {
                "fields" : {
                    "content" : {}
                }
            }
        }
        
        query_hits = self.es_handler.advanced_search(query_body)["hits"]["hits"]
        result = []
        for hit in query_hits:
            doc = Document.from_dict(hit["_source"])
            score = hit["_score"]
            result.append((doc, score))
        return result

    def save(self, query):
        self.es_handler.save()

    @staticmethod
    def load_latest():
        return ElasticSearchIndex()

class GensimIndex(Index):
    """
    Attributes:
        documents (set[str]) : set of document ids
        model_type (str) : name of gensim model
        dictionary (corpora.Dictionary) : dictionary
        model (models.<Name of Model>) : gensim model trained from corpus
        index (similarities.Similarity) : index for lookup
    """
    SAVE_PATH = CONFIG["SAVE_DIR"] + "/index/gensim"

    def __init__(self, tokenizer):

        self.tokenizer = tokenizer
        if not os.path.exists(GensimIndex.SAVE_PATH):
            os.makedirs(GensimIndex.SAVE_PATH)
        self.es_handler = ESHandler()

        self.doc_ids = []
        self.model_type = None
        self.dictionary = None
        self.corpus = None
        self.model = None
        self.index = None
        
    def init(self, model="tfidf"):
        print("Building Gensim Index...")
        self.model_type = model
        documents = self.es_handler.get_all_docs()
        self.doc_ids = [doc.id for doc in documents]
        print("Tokenizing documents...")
        print("Total:", len(documents))
        tokenized_docs = self.tokenizer.tokenize_doc_parallel(documents, 8)

        print("Build dictionary...")
        self.dictionary = corpora.Dictionary(tokenized_docs)
        self.corpus = [self.dictionary.doc2bow(doc) for doc in tokenized_docs]
        corpus_path = GensimIndex.SAVE_PATH + "/mmcorpus"
        corpora.MmCorpus.serialize(corpus_path, self.corpus)
        mmcorpus = corpora.MmCorpus(corpus_path)

        self.model_type = model
        if model == "tfidf":
            self.model = models.TfidfModel(mmcorpus)
        elif model == "lsi":
            self.model = models.LsiModel(mmcorpus)
        elif model == "lda":
            self.model = models.LdaModel(mmcorpus)
        else:
            raise ValueError("Unknown model type:", model)

        index_path = GensimIndex.SAVE_PATH + "/index"
        print("Building index...")
        self.index = similarities.Similarity(
            index_path,
            self.model[mmcorpus],
            len(self.dictionary)
        )
        self.timestamp = str(int(time.time()))
        self.save()
        print("Finished!")

    def update(self):
        new_doc_ids = self.es_handler.get_all_ids()
        difference = list(set(self.doc_ids).difference(set(new_doc_ids)))
        if not difference:
            return 

        print("Updating Gensim Index...")
        self.doc_ids += new_doc_ids
        new_docs = self.es_handler.get_many(difference)
        tokenized_docs = self.tokenizer.tokenize_doc_parallel(new_docs, 2)
        self.dictionary.add_documents(tokenized_docs)
        new_corpus = [dictionary.doc2bow(doc) for doc in tokenized_docs]
        self.corpus += new_corpus
        corpus_path = GensimIndex.SAVE_PATH + "/mmcorpus"
        corpora.MmCorpus.serialize(corpus_path, self.corpus)
        mmcorpus = corpora.MmCorpus(corpus_path)

        model_name = "models." + self.model.__class__.name__
        self.model = eval(model_name) + "(mmcorpus)"

        index_path = GensimIndex.SAVE_PATH + "/index"
        self.index = similarities.Similarity(
            index_path,
            self.model[mmcorpus],
            len(self.dictionary)
        )
        self.timestamp = str(int(time.time()))
        self.save()
        print("Finished!")

    def query(self, query):
        query = self.tokenizer(query)
        bow_rep = self.dictionary.doc2bow(query)
        model_rep = self.model[bow_rep]
        results = self.index[model_rep]
        #Take top 100 results according to similarity score
        top_results = heapq.nlargest(100, enumerate(results), key=lambda item: item[1])
        
        final_results = []

        for result in top_results:
            doc = self.es_handler.get(self.doc_ids[result[0]])
            if doc.title and doc.title != "":
                # Some documents don't have titles, so filter them
                score = float(result[1])
                final_results.append((doc, score))

        return final_results

    def save(self):
        """
            When pickling, manually save dictionary, model, and index
        """
        dict_path = GensimIndex.SAVE_PATH + "/" + self.timestamp + ".dict"
        self.dictionary.save(dict_path)
        model_path = GensimIndex.SAVE_PATH + "/" + self.timestamp + ".model"
        self.model.save(model_path)
        index_path = GensimIndex.SAVE_PATH + "/" + self.timestamp + ".index"
        self.index.save(index_path)

        state = self.__dict__.copy()
        state["dictionary"] = dict_path
        state["model"] = model_path
        state["index"] = index_path
        state["es_handler"] = None

        object_path = GensimIndex.SAVE_PATH + "/" + self.timestamp + ".gensimindex"
        with open(object_path, "wb") as fp:
            pickle.dump(state, fp)

    @staticmethod
    def load(timestamp):
        index = object.__new__(GensimIndex)
        object_path = GensimIndex.SAVE_PATH + "/" + timestamp + ".gensimindex"
        with open(object_path, "rb") as fp:
            state = pickle.load(fp)

        state["dictionary"] = corpora.Dictionary.load(state["dictionary"])
        if state["model_type"] == "tfidf":
            state["model"] = models.TfidfModel.load(state["model"])
        elif state["model_type"] == "lsi":
            state["model"] = models.LsiModel.load(state["model"])
        elif state["model_type"] == "lda":
            state["model"] = models.LdaModel.load(state["model"])
        else:
            raise ValueError("Unknown model type:", state[model_type])

        state["index"] = similarities.MatrixSimilarity.load(state["index"])
        state["es_handler"] = ESHandler()

        index.__dict__ = state
        return index

    @staticmethod
    def load_latest():
        files = [file for file in os.listdir(GensimIndex.SAVE_PATH) 
            if file.endswith(".gensimindex")
        ]
        if not files:
            raise FileNotFoundError("No .gensimindex file")
        latest_timestamp = max(files).replace(".gensimindex", "")
        return GensimIndex.load(latest_timestamp)

