from pprint import pprint

from v2.dal.queries import Queries
from v2.data_loader.fetcher import FetcherCSV
from v2.processor.analyzer import Analyzer


class Manager:
    def __init__(self):
        self.fetcherCSV = FetcherCSV()
        self.data=None
        self.Query=Queries()
        self.dataFromES=None
        self.analyzer=Analyzer()
        self.text=None
        self.finish_processing=False


    def fetcher(self):
        self.data=self.fetcherCSV.fetcher_from_csv()
        # print(self.data)
    def create_index(self):
        self.Query.create_index()

    def insert_data(self):
        # print(self.data)
        self.Query.insert_data(self.data)

    def get_all(self):
        self.dataFromES=self.Query.get_all()
        # for hit in self.dataFromES['hits']['hits']:
        #     pprint(hit['_source'])
        #
    def set_data(self):
        self.analyzer.set_data(self.dataFromES)

    def insert_sentiment(self):
        docs_to_update=self.analyzer.insert_sentiment()
        return docs_to_update

    def update_mapping(self,name):
        self.Query.update_mapping(name)

    def update_fields(self,docs_to_update):
        self.Query.bulk_update_fields(docs_to_update)

    def read_text(self):
        self.text=self.fetcherCSV.read_txt()

    def weapons_in_text(self):
        docs_to_update=self.analyzer.weapons_in_texts(self.text)
        return docs_to_update

    def delete_by_condition(self):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": 0}},
                        {"terms": {"sentiment": ["neutral", "positive"]}}
                    ],
                    "must_not": [
                        {"exists": {"field": "weapons"}}
                    ]
                }
            }
        }

        self.Query.delete_by_condition(query)

    def run(self):
        # self.Query.con_elastic.es.indices.delete(index="tweets", ignore=[400,404])

        self.fetcher()
        self.create_index()
        self.insert_data()
        self.get_all()
        self.set_data()
        # self.update_mapping("sentiment")
        docs_to_update=self.insert_sentiment()
        self.update_fields(docs_to_update)
        self.get_all()
        self.read_text()
        # self.update_mapping("weapons")
        docs_to_update=self.weapons_in_text()
        self.update_fields(docs_to_update)

        self.delete_by_condition()
        self.finish_processing=True
        #

    # {"exists": {"field": "weapons"}}
    def get_antisemitic_with_weapons(self):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": 1}},
                        {"script": {
                            "script": "doc['weapons.keyword'].size() > 0"
                        }}
                    ]
                }
            }
        }
        return self.Query.get_data_by_query(query)


    def get_weapons_more_two(self):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"script": {
                            "script": "doc['weapons.keyword'].size() > 2"
                        }}
                    ]
                }
            }
        }
        return self.Query.get_data_by_query(query)











if __name__ == "__main__":
    m = Manager()
    m.run()
