import nltk

try:
    nltk.corpus.stopwords.words('english')
except LookupError as err:
    print('Falied to import stopwords. Trying to download')

    nltk.download('stopwords')
