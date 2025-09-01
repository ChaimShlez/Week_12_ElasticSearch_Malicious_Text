from pprint import pprint

from elasticsearch import Elasticsearch

class ConnectionElastic:
    def __init__(self):
        self.es = Elasticsearch('http://localhost:9200')
        print(self.es.info())


    def create_index(self,index_name,mappings):
        if not self.es.indices.exists(index=index_name):
          self.es.indices.create(index=index_name, mappings=mappings)


    def get_all(self,index_name,query):
        results = self.es.search(index=index_name, body=query)
        return results

    def update_mapping(self,index_name,new_mapping):
        try:
            self.es.indices.put_mapping(index=index_name, body=new_mapping)
            print(f"Mapping updated successfully for index '{index_name}'.")
        except Exception as e:
            print(f"Failed to update mapping: {e}")



    def add_filed(self,update_doc):
        self.es.update(
            index=update_doc["index"],
            id=update_doc["id"],
            body=update_doc["body"]
        )







