from pymetamap import MetaMap, Concept
from .utils import CONFIG
import os

class NERPipeline():
    def __call__(self, text):
        """Run text through pipeline"""
        raise NotImplementedError

class Metamap(NERPipeline):
    
    def __init__(self):
        self.mm = MetaMap.get_instance(os.path.abspath(CONFIG["MM_PATH"]))
    
    def __call__(self, text):
        concepts, _ = self.mm.extract_concepts([text])
        return [c.cui for c in concepts]

