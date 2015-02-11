#!/usr/bin/env python
"""
Streams and prints named entities from Twitter

Notes
-----
Loads twitter credentials from a file in TWITTER_CONF which must have the keys
TWKEYS set in the section [app].
"""
import configparser
import logging
import nltk
import signal
import sys
import twython

# Set up logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s:%(name)-12s:%(levelname)-8s:%(message)s')
# create logger
log = logging.getLogger('namestream')


TWITTER_CONF = 'conf/twitter.conf'
TWKEYS = ['consumer_key', 'consumer_secret', 'access_token', 'access_token_secret']


class LanguageDetector(object):
    """
    Detect language by requiring a fraction of known words

    Use the == operator to check if some text is of the language in the
    instance.

    Paramters
    ---------
    words : Iterable
        Iterable of str, vocabulary of language
    threshold : Float
        Fraction of known words to classify text as in the language defined by
        the vocabulary
    """

    def __init__(self, words, threshold):
        self.__threshold = threshold
        self.__vocabulary = set(word.lower() for word in words)

    def __eq__(self, text):
        """
        Returns true if text is of the language specified by the vocabulary
        """
        words = set(word.lower() for word in text.split())
        unusual = words - self.__vocabulary
        return len(unusual) <= len(words) * self.__threshold


class NamedEntityStreamer(twython.TwythonStreamer):
    """
    Calls the callback with names found in the twitter stream

    Paramters
    ---------
    callback : callable
        To be called with each name (str)
    *args, **kwargs:
        Arguments for twython.TwythonStreamer

    See Also
    --------
    twython.TwythonStreamer : Base class providing functionality
    """
    english = LanguageDetector(nltk.corpus.words.words(), 0.5)

    def __init__(self, callback, *args, **kwargs):
        self.__callback = callback
        super(NamedEntityStreamer, self).__init__(*args, **kwargs)

    def on_success(self, data):
        """
        Find named entities and call callback
        """
        if 'text' in data:
            text = data['text']
            if self.english == text: 
                tokens = nltk.word_tokenize(text)
                pos = nltk.pos_tag(tokens)
                chunks = nltk.ne_chunk(pos)
                entities = filter(lambda item: isinstance(item, nltk.tree.Tree), chunks)
                people = filter(lambda entity: entity.label() == 'PERSON', entities)
                for p in people:
                    leaves = p.leaves()
                    name = ' '.join(a for a, b in leaves)
                    self.__callback(name)

    def on_error(self, status_code, data):
        """
        Print error code and disconnect
        """
        log.error('Recieved error code: %s' % status_code)
        log.error('Disconnecting from Twitter')
        self.disconnect()


def stream_names(callback, twitter_cred):
    """
    Run the named entity streamer on statuses and print each name

    Paramters
    ---------
    callback : callable
        To be called with each name (str)
    *args, **kwargs:
    twitter_cred : Iterable
        Twitter credentials for twython.TwythonStreamer
    """
    streamer = NamedEntityStreamer(callback, *twitter_cred)

    # Close gracefully on SIGINT
    def signal_handler(signal, frame):
        streamer.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    # start streaming named entities from statuses
    streamer.statuses.filter(track='twitter')


if __name__ == '__main__':
    twconf = configparser.ConfigParser()
    twconf.read(TWITTER_CONF)
    try:
        appconf = twconf['app']
        twitter_cred = tuple(appconf[key] for key in TWKEYS)
    except KeyError:
        log.error('Twitter config not valid')
    else:
        def printer(s):
            print ('%s.\r\n\r\n' % s, end='', flush=True)
        stream_names(printer, twitter_cred)
