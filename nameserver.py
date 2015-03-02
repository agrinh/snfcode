#!/usr/bin/env python
import asynchat
import asyncore
import configparser
import multiprocessing
import namestream
import queue
import socket


class ConnectionHandler(asynchat.async_chat):
    """
    Pushes items from the server's queue to the endpoint

    Items are separated by \n.

    Parameters
    ----------
    sock : socket
        Socket to retrieve request on
    server : NameServer
        Parent server with incoming queue
    """

    def __init__(self, sock, server):
        self.__server = server
        asynchat.async_chat.__init__(self, sock=sock)
        self.set_terminator(b"\r\n\r\n")

    def collect_incoming_data(self, data):
        """
        Dummy
        """

    def found_terminator(self):
        """
        Dummy
        """

    def handle_write(self):
        """
        Push items on queue and allow async_chat to handle writes
        """
        try:
            item = self.__server._queue.get(0)
        except queue.Empty:
            pass
        else:
            self.push(bytes(item + '\n', 'utf-8'))
        return super(ConnectionHandler, self).handle_write()

    def handle_close(self):
        """
        Unregister from server and close
        """
        self.__server.unregister()
        super(ConnectionHandler, self).handle_close()

    def writable(self):
        """
        Writable if items in server queue or async_chat writable
        """
        parent = super(ConnectionHandler, self).writable()
        return parent or not self.__server._queue.empty()


class NameServer(asyncore.dispatcher):
    """
    Connection server listening for incoming connections on addr

    Connections are handled by ConnectionHandler

    Attributes
    ----------
    MAXCONN : int
        Maximum number of connections to accept

    Parameters
    ----------
    sock_spec : tuple
        Tuple with two items specifying (family, type) of socket. See socket
        documentation for details.
    addr : object
        Address for socket. Depends on the family and type, see socket
        documentation.
    queue : queue.Queue
        Queue for incoming messages to send to connected endpoints

    See Also
    --------
    ConnectionHandler : Handles incoming connections
    """
    MAXCONN = 10

    def __init__(self, sock_spec, addr, queue):
        self.__count = 0
        self._queue = queue
        asyncore.dispatcher.__init__(self)
        self.create_socket(*sock_spec)
        self.set_reuse_addr()
        self.bind(addr)
        self.listen(5)

    def handle_accept(self):
        """
        Launch a connection handler and register a connection 

        Ignores if the maximum number of connections is reached
        """
        pair = self.accept()
        if pair is not None and self.__count < self.MAXCONN:
            sock, addr = pair
            self.__count += 1
            handler = ConnectionHandler(sock, self)

    def unregister(self):
        """
        Unregister a connection
        """
        if self.__count <= 0:
            raise ValueError('Nothing to unregister')
        self.__count -= 1


class AsyncoreProcess(multiprocessing.Process):
    """
    Simple class for running asyncore dispatchers in a separate process

    Parameters
    ----------
    *args, **kwargs
        Arguments for parent class

    See Also
    --------
    multiprocessing.Process : Parent class
    """

    def __init__(self, *args, **kwargs):
        self.__inits = list()
        super(AsyncoreProcess, self).__init__(*args, **kwargs)

    def add(self, cls, *args, **kwargs):
        """
        Add an asyncore dispatcher to the process

        Only possible before the process has been started.
        """
        if self.is_alive():
            raise RuntimeError('Cannot add dispatchers when process started')
        self.__inits.append((cls, args, kwargs))

    def run(self):
        """
        Instantiates all added dispatcher classes and rund asyncore.loop
        """
        for cls, args, kwargs in self.__inits:
            cls(*args, **kwargs)
        asyncore.loop(timeout=1)


if __name__ == '__main__':
    twconf = configparser.ConfigParser()
    twconf.read(namestream.TWITTER_CONF)
    try:
        appconf = twconf['app']
        twitter_cred = tuple(appconf[key] for key in namestream.TWKEYS)
    except KeyError:
        log.error('Twitter config not valid')
    else:
        # listening server socket spec
        sockspec = (socket.AF_INET, socket.SOCK_STREAM)
        addr = ('localhost', 9904)

        # start asyncore process
        mpqueue = multiprocessing.Queue(100)
        async = AsyncoreProcess()
        async.add(NameServer, sockspec, addr, mpqueue)
        async.start()
        namestream.stream_names(mpqueue.put, twitter_cred)
