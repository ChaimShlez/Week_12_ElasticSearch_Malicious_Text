from pprint import pprint

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer


class Analyzer:
    def __init__(self):
        self.data=None
        nltk.download('vader_lexicon')
        self.analyzer = SentimentIntensityAnalyzer()



    def set_data(self,dataES):
        self.data=dataES
        # for hit in self.data['hits']['hits']:
        #   pprint(hit['_source'])

    def find_sentiment(self, text):
        score = self.analyzer.polarity_scores(text)
        if score['compound'] >= 0.5:
            return 'positive'
        elif score['compound'] >= -0.49:
            return "neutral"
        else:
            return "negative"

    def insert_sentiment(self):
        docs_to_update = []
        for hit in self.data['hits']['hits']:
            # pprint(hit['_source']['text'])
            score = self.find_sentiment(hit['_source']['text'])
            update_doc = {
                "id": hit["_id"],
                    "doc": {
                        "sentiment": score
                    }

            }

            docs_to_update.append(update_doc)

        return docs_to_update

    def weapons_in_texts(self, weapons):
        print(weapons)
        weapons_list = weapons.split()
        print(weapons_list)
        docs_to_update = []
        for hit in self.data['hits']['hits']:
            words = hit['_source']['text']
            detected = [weapon for weapon in weapons_list if weapon in words]
            update_doc = {
                "id": hit['_id'],
                "doc": {
                    "weapons": detected
                }
            }
            docs_to_update.append(update_doc)
        return docs_to_update

