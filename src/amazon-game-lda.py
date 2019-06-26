'''
The LDA model for topic extraction from Amazon Customer Reviews
Develop & Test on EC2 Jupyter Notebook
'''

import findspark
findspark.init()

import pyspark # Call this only after findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

'''
Read Amazon Customer Reviews Data
'''
reviews = spark.read.parquet("s3n://amazon-reviews-pds/parquet")

reviews_elec = reviews.where("product_category = 'Video_Games' \
                             and marketplace = 'US' \
                             and lower(product_title) like '%pokemon%' \
                              ")

reviews_elec.collect()

df = reviews_elec
df.printSchema()

'''
Stop words processing, preprocess data for NLP modeling
'''

from pyspark.sql.functions import udf,collect_list
from pyspark.sql.types import StringType, ArrayType

from nltk import download, RegexpParser, pos_tag
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import word_tokenize

import string
import re
import itertools


# remove non ASCII characters
def strip_non_ascii(data_str):
    ''' Returns the string without non ASCII characters'''
    stripped = (c for c in data_str if 0 < ord(c) < 127)
    return ''.join(stripped)

strip_non_ascii_udf = udf(strip_non_ascii, StringType())

df = df.withColumn('text_non_asci',strip_non_ascii_udf(df['review_body']))

# modify abbreviations
def fix_abbreviation(data_str):
    data_str = data_str.lower()
    data_str = re.sub(r'\bthats\b', 'that is', data_str)
    data_str = re.sub(r'\bive\b', 'i have', data_str)
    data_str = re.sub(r'\bim\b', 'i am', data_str)
    data_str = re.sub(r'\bya\b', 'yeah', data_str)
    data_str = re.sub(r'\bcant\b', 'can not', data_str)
    data_str = re.sub(r'\bdont\b', 'do not', data_str)
    data_str = re.sub(r'\bwont\b', 'will not', data_str)
    data_str = re.sub(r'\bid\b', 'i would', data_str)
    data_str = re.sub(r'wtf', 'what the fuck', data_str)
    data_str = re.sub(r'\bwth\b', 'what the hell', data_str)
    data_str = re.sub(r'\br\b', 'are', data_str)
    data_str = re.sub(r'\bu\b', 'you', data_str)
    data_str = re.sub(r'\bk\b', 'OK', data_str)
    data_str = re.sub(r'\bsux\b', 'sucks', data_str)
    data_str = re.sub(r'\bno+\b', 'no', data_str)
    data_str = re.sub(r'\bcoo+\b', 'cool', data_str)
    data_str = re.sub(r'rt\b', '', data_str)
    data_str = data_str.strip()
    return data_str

fix_abbreviation_udf = udf(fix_abbreviation, StringType())

def remove_features(data_str):
    # compile regex
    url_re = re.compile('https?://(www.)?\w+\.\w+(/\w+)*/?')
    punc_re = re.compile('[%s]' % re.escape(string.punctuation))
    num_re = re.compile('(\\d+)')
    mention_re = re.compile('@(\w+)')
    alpha_num_re = re.compile("^[a-z0-9_.]+$")
    html_re = re.compile("<br />")
    # convert to lowercase
    data_str = data_str.lower()
    # remove hyperlinks
    data_str = url_re.sub(' ', data_str)
    # remove @mentions
    data_str = mention_re.sub(' ', data_str)
    # remove puncuation
    data_str = punc_re.sub(' ', data_str)
    # remove numeric 'words'
    data_str = num_re.sub(' ', data_str)
    # remove html symbol
    data_str = html_re.sub(' ', data_str)   
    # remove non a-z 0-9 characters and words shorter than 1 characters
    list_pos = 0
    cleaned_str = ''

    for word in data_str.split():
        if list_pos == 0:
            if alpha_num_re.match(word) and len(word) > 1:
                cleaned_str = word
            else:
                cleaned_str = ' '
        else:
            if alpha_num_re.match(word) and len(word) > 1:
                cleaned_str = cleaned_str + ' ' + word
            else:
                cleaned_str += ' '
        list_pos += 1
    return " ".join(cleaned_str.split())

# modifying abbreviations
remove_features_udf = udf(remove_features, StringType())

df = df.withColumn('text_feature_removed',remove_features_udf(df['text_fixed_abbrev']))

df = df.where(df.text_feature_removed.isNotNull())

def lemmatize(data_str):
    # expects a string
    list_pos = 0
    cleaned_str = ''
    lmtzr = WordNetLemmatizer()
    text = data_str.split()
    tagged_words = pos_tag(text)
    for word in tagged_words:
        if 'v' in word[1].lower():
            lemma = lmtzr.lemmatize(word[0], pos='v')
        else:
            lemma = lmtzr.lemmatize(word[0], pos='n')
        if list_pos == 0:
            cleaned_str = lemma
        else:
            cleaned_str = cleaned_str + ' ' + lemma
        list_pos += 1
    return cleaned_str

# lemmatizing words with different tenses and forms
lemmatize_udf = udf(lemmatize, StringType())
lemm_df = df.withColumn("lemm_text", lemmatize_udf(df["text_feature_removed"]))


lemm_df = lemm_df.where(lemm_df.lemm_text.isNotNull())

def tag_and_remove(data_str):
    cleaned_str = ' '
    # noun tags
    nn_tags = ['NN', 'NNP','NNS','NNP','NNPS']
    # adjectives
    jj_tags = ['JJ', 'JJR', 'JJS']
    # verbs
    vb_tags = ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']
    nltk_tags = nn_tags + jj_tags + vb_tags

    # break string into 'words'
    text = data_str.split()
    
    text_notype = []
    for w in text:
        if w is None:
            continue
        else:
            text_notype.append(w)

    # tag the text and keep only those with the right tags
    tagged_text = pos_tag(text_notype)
    
    for i in range(len(tagged_text)):
        if tagged_text[i][1] in nltk_tags:
            if i < len(tagged_text)-1:
                if (tagged_text[i][1] in nn_tags) and (tagged_text[i+1][1] in nn_tags):
                    cleaned_str += tagged_text[i][0] + '_'
                elif (tagged_text[i][1] in jj_tags) and (tagged_text[i+1][1] in nn_tags):
                    cleaned_str += tagged_text[i][0] + '_'  
                else:
                    cleaned_str += tagged_text[i][0] + ' '

    return cleaned_str

# tagging by part of speech
tag_and_remove_udf = udf(tag_and_remove, StringType())

tagged_df = lemm_df.withColumn("tag_text", tag_and_remove_udf(lemm_df.lemm_text))

# filter out the empty non-type values
tagged_df = tagged_df.where(tagged_df.tag_text.isNotNull())

from nltk.corpus import stopwords
download('stopwords')
stop_words = stopwords.words('english')
stop_words.append('br')
stop_words.append('would')


def remove_stops(data_str):
    list_pos = 0
    cleaned_str = ''
    text = data_str.split()
    for word in text:
        if word not in stop_words:
            # rebuild cleaned_str
            if list_pos == 0:
                cleaned_str = word
            else:
                cleaned_str = cleaned_str + ' ' + word
            list_pos += 1
    return cleaned_str

# removing stop words
remove_stops_udf = udf(remove_stops, StringType())

stop_df= tagged_df.withColumn("stop_text", remove_stops_udf(tagged_df["tag_text"]))

# Tokenize reviews into words
tokenize_udf = udf(word_tokenize, ArrayType(StringType()))

token_df = stop_df.withColumn("token_text", tokenize_udf(stop_df["stop_text"]))


from pyspark.sql.functions import collect_list
# filter out the empty non-type values
token_df = token_df.where(token_df.token_text.isNotNull())

df_combine=token_df.groupby('star_rating').agg(collect_list('token_text').alias("review_clean"))


import itertools
# Flatten the nested lists
def flatten_nested_list(nested_list):
    flatten_list = list(itertools.chain.from_iterable(nested_list))
    return flatten_list

flatten_udf = udf(flatten_nested_list, ArrayType(StringType()))

df_combine = df_combine.withColumn('review_cleaned', flatten_udf(df_combine.review_clean))

texts = df_combine.sort("star_rating",ascending=True).select('star_rating','review_cleaned').collect()


#import nltk
#nltk.download('averaged_perceptron_tagger')
#nltk.download('wordnet')
#nltk.download('punkt')


'''
LDA Modeling
'''
documents = []
for i in range(len(texts)):
    documents.append(texts[i].review_cleaned)

# Filter most frequent words
dict_t = {}
for i in documents:
    for j in i:
        if j in dict_t.keys():
            dict_t[j] += 1
        else:
            dict_t[j] = dict_t.get(j, 0) + 1

n_frequent_words=[]
for k, v in dict_t.items():
    if v > 90:
        n_frequent_words.append(k)


documents_filter = []
for l in documents:
    new_l = l
    for w in new_l:
        if w in n_frequent_words:
            new_l.remove(w)
    documents_filter.append(new_l)


from gensim import corpora, models

'''
Create dictionary, convert documents to vectors, LDA model and result
'''
dictionary_1 = corpora.Dictionary([documents_filter[0]])
dictionary_2 = corpora.Dictionary([documents_filter[1]])
dictionary_3 = corpora.Dictionary([documents_filter[2]])


bow_corpus_1 = [dictionary_1.doc2bow(doc) for doc in [documents_filter[0]]]
bow_corpus_2 = [dictionary_2.doc2bow(doc) for doc in [documents_filter[1]]]
bow_corpus_3 = [dictionary_3.doc2bow(doc) for doc in [documents_filter[2]]]


from gensim.models import LdaModel

lda_model_1 = LdaModel(bow_corpus_1, num_topics=3, id2word=dictionary_1, passes=1)
lda_model_2 = LdaModel(bow_corpus_2, num_topics=3, id2word=dictionary_2, passes=1)
lda_model_3 = LdaModel(bow_corpus_3, num_topics=3, id2word=dictionary_3, passes=1)


for idx, topic in lda_model_1.print_topics(-1):
    print('Topic: {} \nWords: {}'.format(idx, topic))
for idx, topic in lda_model_2.print_topics(-1):
    print('Topic: {} \nWords: {}'.format(idx, topic))
for idx, topic in lda_model_3.print_topics(-1):
    print('Topic: {} \nWords: {}'.format(idx, topic))






