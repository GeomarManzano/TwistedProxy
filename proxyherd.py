#! /usr/bin/env python

# Author: Geomar Manzano

import sys, logging, re, json
from project_config import *
from time import time
from twisted.internet import reactor, protocol
from twisted.protocols.basic import LineReceiver
from twisted.web.client import getPage

SERVERS = {
    'Alford':   {'Port': 12200, 'Neighbors': ['Powell', 'Parker']},
    'Bolden':   {'Port': 12201, 'Neighbors': ['Powell', 'Parker']},
    'Hamilton': {'Port': 12202, 'Neighbors': ['Parker']},
    'Parker':   {'Port': 12203, 'Neighbors': ['Alford', 'Bolden', 'Hamilton']},
    'Powell':   {'Port': 12204, 'Neighbors': ['Alford', 'Bolden']}}

# ProxyHerd Server

class ProxyHerdServerProtocol(LineReceiver):
    def __init__(self, factory):
	self.factory = factory
    def connectionMade(self):
	logging.info(' Connection Made.')
    def lineReceived(self, line):
        logging.info(' Line Received')
        self.commandArguments = line.split()
        if self.commandArguments[0] == 'IAMAT': self.IAMAT_handler()
        elif self.commandArguments[0] == 'WHATSAT': self.WHATSAT_handler()
        elif self.commandArguments[0] == 'AT': self.AT_handler()
        else: logging.error(' Invalid Line Received')
    def IAMAT_handler(self):
        if len(self.commandArguments) != 4:
            logging.error(' Invalid number of arguments for IAMAT.')
            self.transport.write('? {0}\n'.format(
                ' '.join(self.commandArguments)))
            return
        command, client_ID, location, client_time = self.commandArguments
        timeDifference = time() - float(client_time)
        if 0 <= timeDifference: timeDifference = '+' + str(timeDifference)
        response = 'AT {0} {1} {2}'.format(
            self.factory.server_name,
            timeDifference,
            ' '.join(self.commandArguments))
        if client_ID in self.factory.clients:
            logging.info(' Updated Client {0}'.format(client_ID))
        else:
            logging.info(' New Client {0}'.format(client_ID))
            self.factory.clients[client_ID] = {
                'msg': response,
                'time': client_time}
            logging.info(' Server Response -> {0}'.format(response))
            self.transport.write('{0}\n'.format(response))
            logging.info(' Location update sent to neighbors.')
            self.updateLocation(response)
    def WHATSAT_handler(self):
        if len(self.commandArguments) != 4:
            logging.error(' Invalid number of arguments for WHATSAT.')
            self.transport.write('? {0}\n'.format(
                ' '.join(self.commandArguments)))
            return
        command, requestedClient_ID, radius, upperBound = self.commandArguments
        if requestedClient_ID in self.factory.clients:
            response = self.factory.clients[requestedClient_ID]['msg']
            logging.info(' Response -> {0}'.format(response))
            (cmd_AT, serverName, timeDifference, cmd_IAMAT, response_client_ID,
             response_location, response_client_time) = response.split()
            response_location = re.sub(r'[-]', ' -', response_location)
	    response_location = re.sub(r'[+]', ' +', response_location).split()
            formattedLocation = (response_location[0] + ',' +
                                 response_location[1])
            API_req = '{0}location={1}&radius={2}&sensor=false&key={3}'.format(
                API_URL,
                formattedLocation,
                radius,
                API_KEY)
            API_resp = getPage(API_req)
            API_resp.addCallback(callback =
                                 lambda arg: (self.dump(arg,
                                                        requestedClient_ID,
                                                        upperBound)))
        else: logging.error(' Nonexistent client was given.')
    def AT_handler(self):
        if len(self.commandArguments) != 7:
            logging.error( ' Invalid number of arguments for AT.')
            return
        (cmd_AT, serverName, timeDifference, cmd_IAMAT, client_ID,
         location, client_time) = self.commandArguments
        if client_ID in self.factory.clients:
            if client_time <= self.factory.clients[client_ID]['time']:
                logging.info(
                    ' Found duplicate location update from Server {0}'.format(
                        serverName))
                return
            logging.info(
                ' Update from Client {0}'.format(client_ID))
        else:
            logging.info('Update from new Client {0}'.format(client_ID))
        self.factory.clients[client_ID] = {
            'msg': ('{0} {1} {2} {3} {4} {5} {6}'.format(
                cmd_AT,
                serverName,
                timeDifference,
                cmd_IAMAT,
                client_ID,
                location,
                client_time)),
            'time': client_time}
        logging.info(' Added Client {0} -> {1}'.format(
            client_ID,
            self.factory.clients[client_ID]['msg']))
        self.updateLocation(self.factory.clients[client_ID]['msg'])
    def dump(self, response, client_ID, upperBound):
	data = json.loads(response)
        data['results'] = data['results'][:int(upperBound)]
	logging.info(' Response: {0}'.format(json.dumps(data, indent = 4)))
	response = '{0}\n{1}\n\n'.format(
            self.factory.clients[client_ID]['msg'],
            json.dumps(data, indent = 4))
	self.transport.write(response)
    def updateLocation(self, message):
        for neighbor in SERVERS[self.factory.server_name]['Neighbors']:
            reactor.connectTCP(
                'localhost',
                SERVERS[neighbor]['Port'],
                ProxyHerdClient(message))
            logging.info(' Server {0} -> Updated from Server {1}'.format(
                neighbor, self.factory.server_name))
    def connectionLost(self, reason):
        logging.info(' Connection Lost.')

class ProxyHerdServer(protocol.ServerFactory):
    def __init__(self, server_name):
        self.server_name = server_name
        self.portNumber = SERVERS[server_name]['Port']
        self.clients = {}
        filename = self.server_name + '.log'
        logging.basicConfig(filename = filename, level = logging.DEBUG) 
        logging.info(' Server {0}: Port {1} started.'.format(
            self.server_name,
            self.portNumber))
    def buildProtocol(self, address):
        return ProxyHerdServerProtocol(self)
    def stopFactory(self):
        logging.info(' Server {0} shutting down.'.format(self.server_name))

# ProxyHerd Client

class ProxyHerdClientProtocol(LineReceiver):
    def __init__(self, factory):
        self.factory = factory
    def connectionMade(self):
        self.sendLine(self.factory.message)
        self.transport.loseConnection()

class ProxyHerdClient(protocol.ClientFactory):
    def __init__(self, message):
        self.message = message
    def buildProtocol(self, addr):
        return ProxyHerdClientProtocol(self)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('One argument is needed.')
        exit()
    factory = ProxyHerdServer(sys.argv[1])
    reactor.listenTCP(SERVERS[sys.argv[1]]['Port'], factory)
    reactor.run()
