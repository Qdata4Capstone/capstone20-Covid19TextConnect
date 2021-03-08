import scispacy
import spacy
from .utils import pip_install
from itertools import chain 
import multiprocessing as mp

class Tokenizer():

    def __call__(self, text):
        raise NotImplementedError()
        
class SciSpacyTokenizer(Tokenizer):
    MODEL_NAME = "en_core_sci_md"
    MODEL_URL = "https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.4/en_core_sci_md-0.2.4.tar.gz"
    MAX_LENGTH = 2000000
    
    def __init__(self):
        try:
            self.model = spacy.load(SciSpacyTokenizer.MODEL_NAME)
        except OSError:
            pip_install(SciSpacyTokenizer.MODEL_URL)
            self.model = spacy.load(SciSpacyTokenizer.MODEL_NAME)

        self.model.max_length = SciSpacyTokenizer.MAX_LENGTH

    def __call__(self, text):
        """
        Args:
            text (str) : text to preprocess
        Returns:
            Token list obtained using scispacy
        """

        doc = self.model(text)

        tokens = []
        for token in doc:
            if token.is_stop:
                continue
            tokens.append(token.lemma_)

        return tokens

    def __getstate__(self):
        return {}

    def __setstate__(self, state):
        self.__init__()

    def tokenize_doc(self, doc):
        return self.__call__(doc.text)

    def tokenize_doc_batch(self, documents):
        tokenized_docs = []
        for doc in documents:
            tokenized_docs.append(self.__call__(doc.text))
        return tokenized_docs

    def tokenize_doc_parallel(self, documents, workers=4):

        with mp.Pool(processes=workers) as pool:
            batch_size = len(documents) // workers
            start = 0
            doc_batches = []
            i = 0
            for i in range(workers):
                if i == workers - 1:
                    batch_size += len(documents) % workers
                
                end = start + batch_size
                doc_batches.append(documents[start:end])
                start = end

            tokenized_docs = pool.map(
                self.tokenize_doc_batch,
                doc_batches
            )
        # Flatten list
        tokenized_docs = list(chain.from_iterable(tokenized_docs))
        return tokenized_docs

        

