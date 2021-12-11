import os
import pysolr
import requests
import pickle

CORE_NAME = "TwitterData"
AWS_IP = "localhost"


def delete_core(core=CORE_NAME):
    print(os.system('sudo su - solr -c "/opt/solr/bin/solr delete -c {core}"'.format(core=core)))


def create_core(core=CORE_NAME):
    print(os.system(
        'sudo su - solr -c "/opt/solr/bin/solr create -c {core} -n data_driven_schema_configs"'.format(
            core=core)))


class Indexer:
    def __init__(self):
        self.solr_url = f'http://{AWS_IP}:8983/solr/'
        self.connection = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)

    def do_initial_setup(self):
        delete_core()
        create_core()

    def create_documents(self, docs):
        self.connection.add(docs)

    def add_fields(self):
        data = {
            "add-field": [
                {
                    "name": "poi_name",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "poi_id",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "verified",
                    "type": "boolean",
                    "multiValued": False
                },
                {
                    "name": "country",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "replied_to_tweet_id",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "replied_to_user_id",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "reply_text",
                    "type": "text_general",
                    "multiValued": False
                },
                {
                    "name": "tweet_text",
                    "type": "text_general",
                    "multiValued": False
                },
                {
                    "name": "tweet_lang",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "text_en",
                    "type": "text_en",
                    "multiValued": False
                },
                {
                    "name": "text_hi",
                    "type": "text_hi",
                    "multiValued": False
                },
                {
                    "name": "text_es",
                    "type": "text_es",
                    "multiValued": False
                },

                {
                    "name": "hashtags",
                    "type": "strings",
                    "multiValued": True
                },
                {
                    "name": "mentions",
                    "type": "strings",
                    "multiValued": True
                },
                {
                    "name": "tweet_urls",
                    "type": "strings",
                    "multiValued": True
                },
                {
                    "name": "tweet_emoticons",
                    "type": "strings",
                    "multiValued": True
                },

                {
                    "name": "tweet_date",
                    "type": "pdate",
                    "multiValued": False
                },

                {
                    "name": "tweet_translation",
                    "type": "text_general",
                    "multiValued": False
                },
                {
                    "name": "sentiment",
                    "type": "pfloat",
                    "multiValued": False
                },
                {
                    "name": "sentiment_level",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "topic",
                    "type": "text_general",
                    "multiValued": False
                },
                {
                    "name": "topic_score",
                    "type": "pfloat",
                    "multiValued": False
                }
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


if __name__ == "__main__":
    with open('tweets_updated.pickle', 'rb') as f:
        collection = pickle.load(f)

    i = Indexer()
    i.do_initial_setup()
    i.add_fields()
    i.create_documents(collection)
