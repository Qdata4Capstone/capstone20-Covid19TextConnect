import json
from pymetamap.Concept import ConceptMMI

class Document:
    '''
    Class used to represent a document

    Attributes:
    id : str
        Unique id to identify the document.
    title: str
        Title of document
    metadata : dict [str, Union[list[str], str]]
        Dictionary containing metadata. 
        Ex: {"authors": ["John Smith", "Alic Smith"], "date": "2020-03-10", "doi": "10.1000/xyz123"}
    content : dict [str, str]
        Actual content of the document. Dictionary is used to categorize type of text.
        Ex: {"abstract": "...<abstract>...", "body": "...<body>...", "supplementary": "...<figure caption>..."}
    annotations : dict[list] 
        Annotations provided by NER tools
        Ex: {"bern": [<bern tags>]}
    '''
    def __init__(self, id, title, metadata, content):
        self.id = id
        self.title = title
        self.metadata = metadata
        self.content = content
        self.annotations = {}

    def _metadata_to_str(self):
        """ Return metadata dictionary to string representation """
        text = ""
        for key, value in self.metadata.items():
            text += key + ": "
            if type(value) is list:
               text += ",".join(value)
            elif type(value) is str:
                text += value
            text += "\n"
        return text

    def _content_to_str(self):
        """ Return content dictionary to string representation """
        text = ""
        for value in self.content.values():
            if type(value) is str:
                text += value + "\n"
        return text[:-1]

    @property
    def text(self):
        """ Return raw text representation of document """
        text = self.title + "\n" + self._metadata_to_str() \
            + self._content_to_str()
        return text

    def to_json(self):
        """ Serialize Document into form of JSON string """
        return json.dumps(self.__dict__)

    def to_dict(self):
        return self.__dict__
    
    @staticmethod
    def from_json(json_str):
        """ Load JSON string and return new instance of Document """
        doc = Document(None,None,None,None)
        state = json.loads(json_str)
        if "metamap" in state["annotations"]:
            for i in range(len(state["annotations"]["metamap"])):
                concept = state["annotations"]["metamap"][i]
                state["annotations"]["metamap"][i] = ConceptMMI(*concept)

        doc.__dict__ = state
        return doc

    @staticmethod
    def from_dict(state):
        """ Load Python dict and return new instance of Document """
        doc = Document(None,None,None,None)
        if "metamap" in state["annotations"]:
            for i in range(len(state["annotations"]["metamap"])):
                concept = state["annotations"]["metamap"][i]
                state["annotations"]["metamap"][i] = ConceptMMI(*concept)
        doc.__dict__ = state
        return doc
