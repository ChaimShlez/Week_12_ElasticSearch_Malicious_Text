from pprint import pprint

from elasticsearch import helpers

from v2.dal.connection_elastic import ConnectionElastic


class Queries:
    def __init__(self):
        self.con_elastic=ConnectionElastic()
        self.name_index="tweets"



    def create_index(self):
        # self.con_elastic.es.indices.delete(index=index_name)
        mappings = {
                "properties": {
                    "TweetID": {
                        "type": "keyword"
                    },
                    "CreateDate": {
                        "type": "keyword"
                    },
                    "Antisemitic": {
                        "type": "keyword"
                    },
                    "text": {
                        "type": "text",

                    }
                }
        }

        try:
            print("SDaxdad as")
            res=self.con_elastic.create_index(self.name_index,mappings)
            print(res)
        except Exception as e:
            print(f"error{e}")

    def insert_data(self, data):
        actions = [
            {
                "_index": self.name_index,
                "_source": doc
            } for doc in data
        ]

        response = helpers.bulk(self.con_elastic.es, actions)
        print(response)

    def get_all(self):
        query = {
            "query": {
                "match_all": {}
            },
            "size": 10000

        }
        results = self.con_elastic.get_all(self.name_index, query)
        # for hit in results['hits']['hits']:
        #     pprint(hit['_source'])
        # print("---------------------------------------------------------------------------------------------")

        return results

    def update_mapping(self,name_field):
        new_mappings = {
            "properties": {
                name_field: {"type": "keyword"}
            }
        }

        self.con_elastic.update_mapping(self.name_index, new_mappings)

    def bulk_update_fields(self, docs_to_update):
        actions = [
            {
                "_op_type": "update",
                "_index": self.name_index,
                "_id": doc["id"],
                "doc": doc["doc"]
            }
            for doc in docs_to_update
        ]
        try:
            success, errors = helpers.bulk(self.con_elastic.es, actions)
            print(f"Updated {success} documents")
        except Exception as e:
            print(f"Failed to update documents: {e}")

    def delete_by_condition(self, conditions):
        try:



            res = self.con_elastic.es.delete_by_query(index=self.name_index, body=conditions)
            print(f"Deleted {res['deleted']} documents")
        except Exception as e:
            print(f"Failed to delete documents from index {self.name_index}: {e}")
            return 0

    def get_antisemitic_with_weapons(self, query):
        res = self.con_elastic.es.search(index=self.name_index, body=query)
        return [hit['_source'] for hit in res['hits']['hits']]

