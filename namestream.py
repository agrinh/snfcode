#!/usr/bin/env python
"""
Streams and prints named entities from english tweets on Twitter

Arguments are tracked. E.g. to track #bieber:
> ./namestream.py #bieber

Notes
-----
Loads twitter credentials from a file in TWITTER_CONF which must have the keys
TWKEYS set in the section [app].
"""
import configparser
import logging
import nltk
import requests
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

    def __init__(self, callback, *args, **kwargs):
        self.__callback = callback
        super(NamedEntityStreamer, self).__init__(*args, **kwargs)

    def on_success(self, data):
        """
        Find named entities and call callback
        """
        if 'text' in data:
            text = data['text']
            tokens = nltk.word_tokenize(text)
            pos = nltk.pos_tag(tokens)
            chunks = nltk.ne_chunk(pos)
            entities = filter(lambda item: isinstance(item, nltk.tree.Tree), chunks)
            people = filter(lambda entity: entity.label() == 'PERSON', entities)
            for p in people:
                leafs = p.leaves()
                if len(leafs) == 2 or len(leafs) == 3:
                    name = ' '.join(a for a, b in leafs)
                    self.__callback(name)

    def on_error(self, status_code, data):
        """
        Print error code and disconnect
        """
        log.error('Recieved error code: %s' % status_code)
        log.error('Disconnecting from Twitter')
        self.disconnect()


def stream_names(callback, twitter_cred, **twargs):
    """
    Run the named entity streamer on statuses and print each name

    Restarts the stream if a requests.exceptions.RequestException is raised.

    Paramters
    ---------
    callback : callable
        To be called with each name (str)
    twitter_cred : Iterable
        Twitter credentials for twython.TwythonStreamer
    **twargs
        Filter parameters for twitter filter streaming
    """
    streamer = NamedEntityStreamer(callback, *twitter_cred)

    # Close gracefully on SIGINT
    def signal_handler(signal, frame):
        streamer.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    # start streaming named entities from statuses
    while True:
        try:
            streamer.statuses.filter(**twargs)
        except requests.exceptions.RequestException:
            continue
        else:
            break


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
            print ('%s.\r\n\r\n' % (s, ), end='', flush=True)
        track = ' '.join(sys.argv[1:])
        print('Tracking: %s' % track)
        stream_names(printer, twitter_cred, language='en', track=track)
