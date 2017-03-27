# Copyright 2015, Google Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

""" Python implementation of the GRPC-based service - both insecure and SSL-based """

from concurrent import futures
import time, json, grpc, random

from DBManager import DBManager
import yaml
import mysql.connector

import helloworld_echo_service_pb2
import helloworld_echo_service_pb2_grpc

from grpc._cython import cygrpc as _cygrpc


_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_MAX_WORKERS = 1



def retrieve_from_CES(transport_protocol, link_alias, direction, ces_fqdn):
	dbManager_CES = DBManager("127.0.0.1","root","take5","CES_Policies")
	dbManager_CES.connect()
	response =  dbManager_CES.retrieve_from_ces(transport_protocol, link_alias, direction, ces_fqdn)
	dbManager_CES.deconnect()
	return response

def retrieve_from_msisdn(reply):
	dbManager_Firewall = DBManager("127.0.0.1","root","take5","Firewall_Policies")
	dbManager_Firewall.connect()
	response =  dbManager_Firewall.retrieve_from_msisdn(str(reply))
	with open('data.yaml', 'w') as outfile:
		yaml.dump(response, outfile,default_flow_style=False)
	dbManager_Firewall.deconnect()
	return response

def retrieve_from_fqdn(reply):
	dbManager_Firewall = DBManager("127.0.0.1","root","take5","Firewall_Policies")
	dbManager_Firewall.connect()
	response =  dbManager_Firewall.retrieve_from_fqdn(str(reply))
	dbManager_Firewall.deconnect()
	return response

def retrieve_from_host(local_fqdn, remote_fqdn, direction):
	dbManager_CES = DBManager("127.0.0.1","root","take5","CES_Policies")
	dbManager_CES.connect()
	response =  dbManager_CES.retrieve_from_host(local_fqdn, remote_fqdn, direction)
	dbManager_CES.deconnect()
	return response



class Greeter(helloworld_echo_service_pb2_grpc.GreeterServicer):
    def SayHello(self, request, context):
        print ('-------',request)
        x=1
        req=request
        if x==1: #for req in request:
            parsing=json.loads(req.name)
            if parsing['type']=='Host':
                local_fqdn = parsing['query_value'][0]
                remote_fqdn = parsing['query_value'][1]
                direction = parsing['query_value'][2]
                reply = retrieve_from_host(local_fqdn, remote_fqdn, direction)
            elif parsing['type']=='Firewall':
                if parsing['query_parameters'][0] == 'msisdn':
                    parsing = parsing['query_value'][0]
                    reply = retrieve_from_msisdn(parsing)
                elif parsing['query_parameters'][0] == 'fqdn':
                    parsing = parsing['query_value'][0]
                    reply = retrieve_from_fqdn(parsing)
            elif parsing['type']=='CES':
                transport_protocol = parsing['query_value'][0]
                link_alias = parsing['query_value'][1]
                direction = parsing['query_value'][2]
                ces_fqdn = parsing['query_value'][3]
                reply = retrieve_from_CES(transport_protocol, link_alias, direction, ces_fqdn)
            else:
                reply= 'Incorrect Format or Table selection. Please use the following format [[Database Type], [Paramters], [Values]]'

            rows = json.dumps(reply)
            print ('Reply Sent to Client = ', rows)
            return helloworld_echo_service_pb2.HelloReply(message=rows)

def start_serving():
    """ Unprotected gRPC server """
    ip_addr, port = '[::]', 50061                   # IPv6 address shall be provided in square brackets.
    addr = ip_addr + ':' + str(port)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS))
    helloworld_echo_service_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)    
    server.add_insecure_port(addr)
    server.start()
    
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


def start_serving_securely():
    """ SSL/TLS based gRPC server """
    key=open('certificate_store/server.key').read()
    crt=open('certificate_store/server.crt').read()
    
    ip_addr, port = '[::]', 50061
    addr = ip_addr + ':' + str(port)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS))
    helloworld_echo_service_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
        
    server_credentials = grpc.ssl_server_credentials([(key.encode(), crt.encode())]) 
    server.add_secure_port(addr, server_credentials)
    server.start()
    
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


def start_serving_securely2():
    """ SSL/TLS based gRPC server - Mutual Authentication (where server authenticates the client as well) """
    
    key=open('certificate_store/server.key').read()
    crt=open('certificate_store/server.crt').read()
    root_crt = open('certificate_store/CA.crt').read()
    
    ip_addr, port = 'localhost', 50061
    addr = ip_addr + ':' + str(port)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS))
    helloworld_echo_service_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
        
    server_credentials = grpc.ssl_server_credentials([(key.encode(), crt.encode())], root_certificates=root_crt.encode(), require_client_auth=True) 
    server.add_secure_port(addr, server_credentials)
    server.start()
    
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    start_serving()
    #start_serving_securely()
    #start_serving_securely2()
    
