import scispacy
import spacy

class PrePreprocessor():

    def __call__(self, text):
        raise NotImplementedError()
        
class SciSpacyPreProcessor(PrePreprocessor):
    MODEL_NAME = "en_core_sci_md"
    MAX_LENGTH = 2000000
    
    def __init__(self):
        self.model = spacy.load(SciSpacyPreProcessor.MODEL_NAME)
        self.model.max_length = SciSpacyPreProcessor.MAX_LENGTH

    def __call__(self, text):
        """
        Process text through a preprocessing pipeline
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

    def process_doc(self, doc):
        return self.__call__(doc.full_text)

    def multiprocess_doc(self, documents, workers=4):

        with mp.Pool(processes=workers) as pool:
            batch_size = len(documents) // workers
            start = 0
            document_batch = []
            for i in range(num_core):
                if i == num_core - 1:
                    batch_size += len(self.documents) % num_core
                
                end = start + batch_size
                document_batch.append(self.documents[start:end])
                start = end

            tokenized_docs = pool.map(
                self.preprocessor.batch_call,
                document_batch
            )
        # Flatten list
        tokenized_docs = list(chain.from_iterable(tokenized_docs))
    

    def batch_call(self, doc_list):
        token_list = []
        for doc in doc_list:
            token_list.append(self.__call__(doc.full_text()))
        return token_list



        

