import nltk
from nltk.tokenize import sent_tokenize

from tqdm.auto import tqdm

import re
import string
from nltk.corpus import stopwords

from tqdm.notebook import tqdm
tqdm.pandas()

nltk.download('punkt')
nltk.download('stopwords')

def remove_urls(text):
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    return url_pattern.sub(r'', text)


PUNCT_TO_REMOVE = string.punctuation
def remove_punctuation(text):
    """custom function to remove the punctuation"""
    return ''.join([i for i in text if i not in PUNCT_TO_REMOVE])

def remove_non_words(text):
    return ' '.join(re.findall(r'\b[^\d\W]+\b', text))

STOPWORDS = set(stopwords.words('english'))
STOPWORDS.add('fig')
def remove_stopwords(text):
    """custom function to remove the stopwords"""
    return " ".join([word for word in str(text).split() if word not in STOPWORDS])

def remove_short_words(text):
    return " ".join([i for i in str(text).split() if len(i) >= 3])


def preprocess(text):
    pipeline = (remove_urls, remove_punctuation, remove_non_words, remove_stopwords, remove_short_words)
    for func in pipeline:
        text = func(text)
    return text


def split_to_sentences(text):
    return sent_tokenize(text)