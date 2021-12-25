import pickle

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from googletrans import Translator

translator = Translator()
analyser = SentimentIntensityAnalyzer()


def get_sentiment_level(polarity_):
    if polarity_ <= -0.6:
        return 'strongly_negative'
    elif polarity_ <= -0.2:
        return 'moderately_negative'
    elif polarity_ <= 0.2:
        return 'neutral'
    elif polarity_ <= 0.6:
        return 'moderately_positive'
    else:
        return 'strongly_positive'


with open('tweets_hi.pickle', 'rb') as f:
    d = pickle.load(f)

d = [_ for _ in d if _['tweet_lang'] != 'en']

for _, dict_ in enumerate(d):
    try:
        if dict_['tweet_lang'] != 'en':
            if dict_['tweet_lang'] == 'hi':
                if len(dict_['text_hi']) >= 5:
                    d[_]['tweet_translation'] = translator.translate(dict_['text_hi']).text
                    d[_]['sentiment'] = analyser.polarity_scores(dict_['tweet_translation'])['compound']
                    d[_]['sentiment_level'] = get_sentiment_level(dict_['sentiment'])
            else:
                if len(dict_['text_es']) >= 5:
                    d[_]['tweet_translation'] = translator.translate(dict_['text_es']).text
                    d[_]['sentiment'] = analyser.polarity_scores(dict_['tweet_translation'])['compound']
                    d[_]['sentiment_level'] = get_sentiment_level(dict_['sentiment'])

        else:
            d[_]['sentiment'] = analyser.polarity_scores(dict_['text_en'])['compound']
            d[_]['sentiment_level'] = get_sentiment_level(dict_['sentiment'])
    except Exception as e:
        with open('logs_hi.txt', 'a') as f:
            f.write('{}\n'.format(e))
        pass

    with open('logs_hi.txt', 'a') as f:
        f.write('==========={} Done===========\n'.format(_))

with open('tweets_sent_hi.pickle', 'wb') as f:
    pickle.dump(d, f, protocol=pickle.HIGHEST_PROTOCOL)
