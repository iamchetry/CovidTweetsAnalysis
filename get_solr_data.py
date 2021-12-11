import pickle
import requests
import time
import pandas as pd

#s = 'http://18.191.251.215:8983/solr/TwitterData/select?q=covid&fl=id%20poi_name%20verified%20tweet_text%20tweet_lang%20text_en%20text_hi%20text_es%20tweet_translation%20sentiment%20sentiment_level%20hashtags%20mentions%20tweet_emoticons%20tweet_date&defType=edismax&qf=tweet_text&wt=json&indent=true&rows=10000'


with open('crowdsourced_keywords.pickle', 'rb') as f:
    kwd_dict = pickle.load(f)

kwd_list = kwd_dict['covid']

id_list = list()
doc_list = list()

t1 = time.time()

for _, q in enumerate(kwd_list):
    q = q + ' ~10'
    print(q)
    inurl = 'http://18.191.251.215:8983/solr/TwitterData/select?q={}&fl=id%20poi_name%20verified%20tweet_text%20tweet_lang%20text_en%20text_hi%20text_es%20tweet_translation%20sentiment%20sentiment_level%20hashtags%20mentions%20tweet_emoticons%20tweet_date&defType=edismax&qf=tweet_text&wt=json&indent=true&rows=10000'.format('%20'.join(q.split()))

    data = eval(requests.get(inurl).text.replace('true', 'True').replace('false', 'False'))
    docs = data['response']['docs']
    if docs:
        for d in docs:
            id_ = int(d['id'])
            d['id'] = int(d['id'])
            if id_ not in id_list:
                id_list.append(id_)
                doc_list.append(d)

    print(len(doc_list))

    if len(doc_list) >= 70000:
        break

t2 = time.time()
print((t2-t1)/60)
print(doc_list[0])
doc_list = pd.DataFrame(doc_list)

# with open('tweets_d.pickle', 'wb') as f:
#     pickle.dump(doc_list, f, protocol=pickle.HIGHEST_PROTOCOL)

doc_list.to_csv('df_tweets_replies.csv', index=False)

#q = 'http://18.191.251.215:8983/solr/TwitterData/select?q=covid%20vaccine%20~10&fl=id%20poi_name%20poi_id%20replied_to_tweet_id%20replied_to_user_id%20tweet_lang%20sentiment_level%20tweet_date&defType=edismax&qf=tweet_text&wt=json&indent=true&rows=10000'