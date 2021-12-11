import os
import numpy as np
import pandas as pd
import pickle
import string

import gensim
from gensim.utils import simple_preprocess
import nltk
from nltk.stem.snowball import PorterStemmer
from nltk.corpus import stopwords

STOPWORDS = set(stopwords.words('english'))
stemmer = PorterStemmer()
lemmatizer = nltk.wordnet.WordNetLemmatizer()


def lda_model(corpus=None, num_topics=None, id2word=None, passes=None, workers=None):
    return gensim.models.LdaMulticore(corpus, num_topics=num_topics, id2word=id2word, passes=passes, workers=workers)


def get_lemmatized_text(text):
    text_v = lemmatizer.lemmatize(text, pos='v')
    if text_v != text:
        return text_v
    else:
        text_ad = lemmatizer.lemmatize(text, pos='a')
        if text_ad != text:
            return text_ad
        else:
            text_n = lemmatizer.lemmatize(text, pos='n')
            if text_n != text:
                return text_n
            else:
                return text


def tokenize_text(text, min_word_length=None, is_word2vec=False):
    if not is_word2vec:
        return [stemmer.stem(get_lemmatized_text(_)) for _ in simple_preprocess(text) if _ not in STOPWORDS
                and len(_) >= min_word_length]
    else:
        return [_ for _ in text.translate(str.maketrans('', '', string.punctuation)).strip().split()
                if _.lower() not in STOPWORDS and len(_) >= min_word_length]


def get_bow_corpus(data_, column, min_word_length=None):
    corpus = data_[column].apply(lambda _: tokenize_text(_, min_word_length=min_word_length))
    id2word = gensim.corpora.Dictionary(corpus)
    return [id2word.doc2bow(_) for _ in corpus], id2word


def get_coherence(model=None, corpus=None, coherence_method=None):
    return gensim.models.coherencemodel.CoherenceModel(model=model, corpus=corpus,
                                                       coherence=coherence_method).get_coherence()


def get_coherence_for_lda_models(corpus=None, topics_list=None, id2word=None, coherence_method=None, passes=None,
                                 workers=None):
    coherence_list = list()
    for topic_ in topics_list:
        model_ = lda_model(corpus=corpus, num_topics=topic_, id2word=id2word, passes=passes, workers=workers)
        coh_ = get_coherence(model=model_, corpus=corpus, coherence_method=coherence_method)
        coherence_list.append(coh_)

    return dict(zip(topics_list, coherence_list))


def get_optimal_topic(dict_):
    keys_ = list(dict_.keys())
    values_ = list(dict_.values())
    return keys_[np.argmax(values_)]


def dump_as_pickle(object=None, filename=None):
    with open(filename, 'wb') as file_:
        pickle.dump(object, file_)


def load_from_pickle(filename=None):
    with open(filename, 'rb') as file_:
        return pickle.load(file_)


def predict_topic_scores(data_, model_lda=None, corpus=None, temp_score_col=None, topic_dim=None):
    data_[temp_score_col] = [[round(__[-1], 3) for __ in model_lda[_]] for _ in corpus]
    return data_

    # return pd.concat([data_.drop(columns=temp_score_col, axis=1), (pd.DataFrame(
    #     data_[temp_score_col].tolist(), index=data_.index,
    #     columns=['topic_{}_score'.format(_) for _ in range(1, topic_dim + 1)])).applymap(lambda x: round(x, 3))],
    #                  axis=1, ignore_index=False)


if __name__ == '__main__':
    with open('tweets_updated.pickle', 'rb') as f:
        data_ = pickle.load(f)

    data_ = pd.DataFrame(data_)[['id', 'tweet_translation']]
    print('===================================')

    train_corpus, id2word = get_bow_corpus(data_, 'tweet_translation', min_word_length=4)
    with open('train_corpus.pickle', 'wb') as f:
        pickle.dump(train_corpus, f, protocol=pickle.HIGHEST_PROTOCOL)

    with open('id2word.pickle', 'wb') as f:
        pickle.dump(id2word, f, protocol=pickle.HIGHEST_PROTOCOL)

    print('================= 2nd done ==================')

    with open('train_corpus.pickle', 'rb') as f:
        train_corpus = pickle.load(f)

    with open('id2word.pickle', 'rb') as f:
        id2word = pickle.load(f)

    coh_dict = get_coherence_for_lda_models(corpus=train_corpus, topics_list=[3, 4, 5, 6, 7, 9, 11], id2word=id2word,
                                            coherence_method='u_mass', passes=1,
                                            workers=os.cpu_count() - 1)

    with open('coh_dict.pickle', 'rb') as f:
        coh_dict = pickle.load(f)

    topic_number = get_optimal_topic(coh_dict)
    print(topic_number)

    model_lda = lda_model(corpus=train_corpus, num_topics=topic_number, id2word=id2word, passes=1,
                          workers=os.cpu_count() - 1)

    with open('model_lda.pickle', 'wb') as f:
        pickle.dump({'model': model_lda, 'no_of_topics': topic_number}, f, protocol=pickle.HIGHEST_PROTOCOL)

    with open('model_lda.pickle', 'rb') as f:
        model_lda = pickle.load(f)

    topic_number = model_lda['no_of_topics']
    model_lda = model_lda['model']

    data_ = predict_topic_scores(data_, model_lda=model_lda, corpus=train_corpus, temp_score_col='topic_scores',
                                 topic_dim=topic_number)
    data_.to_csv('df_topic_scores.csv', index=False)
