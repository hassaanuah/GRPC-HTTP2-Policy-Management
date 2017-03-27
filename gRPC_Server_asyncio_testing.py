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
import time
import grpc
import asyncio

import helloworld_echo_service_pb2
import helloworld_echo_service_pb2_grpc

from grpc._cython import cygrpc as _cygrpc


_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class iCESClient:
    def __init__(self):
        self.cnt = 0
    
    def resp(self, req):
        try:
            time.sleep(0.5)        
            return req
        except Exception as msg:
            print(msg)

    @asyncio.coroutine
    def resp2(self, req):
        try:
            yield from asyncio.sleep(1)
            #future.set_result()
            return helloworld_echo_service_pb2.HelloReply(message='Hello, %s!' % req)
        except Exception as msg:
            print(msg)


class Greeter(helloworld_echo_service_pb2_grpc.GreeterServicer):
    def __init__(self):
        self.ices_object = {}
        self.loop = asyncio.get_event_loop()
        
    def has(self, peer_id):
        return peer_id in self.ices_object
    
    def get(self, peer_id):
        if self.has(peer_id):
            return self.ices_object[peer_id]
        
    def add(self, peer_id, ices_obj):
        self.ices_object[peer_id]=ices_obj
    
    #"""   
    #Normal gRPC Service implementation 
    def SayHello(self, request, context):
        #grpc.ServicerContext
        #n = context.invocation_metadata()        # information about client user-agent & gRPC version.
        #print(n)
        #context.cancel()en(self.ices_object)
        
        peer_id = context.peer()
        if self.has(peer_id):
            ices_obj = self.get(peer_id)
        else:
            ices_obj = iCESClient()
            self.add(peer_id, ices_obj)
        
        print("Message received", request.name)
        resp = ices_obj.resp(request.name)
        return helloworld_echo_service_pb2.HelloReply(message='Hello, %s!' % resp)      # This desired response can be sent as callback
    #"""
    
        
    '''
    # An attempt at using asyncio with grpc-service
    def SayHello(self, request, context):
        # Testing for asyncio at server side
        print("Start")
        peer_id = context.peer()
        if self.has(peer_id):
            ices_obj = self.get(peer_id)
        else:
            ices_obj = iCESClient()
            self.add(peer_id, ices_obj)
        
        asyncio.set_event_loop(self.loop)                           # To avoid problem of not finding any loop.
        t = asyncio.ensure_future(ices_obj.resp2(request.name))
        
        print("End")
        return helloworld_echo_service_pb2.HelloReply(message='Hello, %s!' % t.result())
    '''
    
    '''
    # Another attempt at using asyncio with gRPC-service 
    @asyncio.coroutine
    def SayHello(self, request, context):
        # Testing for asyncio at server side
        print("Start")
        peer_id = context.peer()
        if self.has(peer_id):
            ices_obj = self.get(peer_id)
        else:
            ices_obj = iCESClient()
            self.add(peer_id, ices_obj)
        
        client_task = self.loop.create_task(ices_obj.resp(request.name))
        resp = yield from client_task                    # Not possible, as SayHello isn't a coroutine itself.
                                                  
        return helloworld_echo_service_pb2.HelloReply(message='Hello, %s!' % resp)
        #context.cancel()
    '''
    

class grpcServerAPI:
    def __init__(self, max_workers=1):
        self._max_workers = max_workers                            # Add whatever seems fit

    def create_server(self, ip_addr, port):
        """
        ip_addr and port of the listening service.    [̣::] - IPv6 address are provided in square brackets
        """
        self._ip, self._port = ip_addr, port
        self._addr = self._ip + ':' + str(self._port)
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=self._max_workers))
        self._server.add_insecure_port(self._addr)
        return self._server    
    
    def add_grpc_service(self, grpc_service_file=None, grpc_server_class=None):
        """ 
        grpc_service_file:        - Name of the file generated by gRPC upon compilation of service.proto file.
        grpc_server_class:        - The class servicing the inbound requests - It is originally sub-classed from the servicer class present in the 'grpc_service_file'        
        """
        if (grpc_service_file is None) or (grpc_server_class is None):
            print("gRPC service class is not provided")
            
        try:
            grpc_service_file.add_GreeterServicer_to_server(grpc_server_class(), self._server)            
        except Exception as msg:
            print("Error adding the gRPC service to server")

    
    def create_secure_server(self, ip_addr, port, priv_key=None, cert=None, root_cert=None, require_client_auth=False):
        """
        ip_addr and port of the listening service.    [̣::] - IPv6 address are provided in square brackets
        priv_key, cert          - Private_key, Certificate of the server
        root_cert               - Root certificate that issued the server 'certificate' or any subsidary CA that issued the certificate
        require_client_auth:    - Boolean indicating whether Client Authentication is needed at SSL/TLS
         
        Provide key, cert and root_cert in following format - key=open('certificate_store/server.key').read()
        """        
        self._ip, self._port = ip_addr, port
        self._addr = self._ip + ':' + str(self._port)
        self._priv_key, self._cert, self._root_cert = priv_key, cert, root_cert
        
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=self._max_workers))
        self._server_credentials = grpc.ssl_server_credentials([(self._priv_key.encode(), self._cert.encode())], root_certificates=self._root_cert.encode(), require_client_auth=require_client_auth)
        self._server.add_secure_port(self._addr, self._server_credentials)
        return self._server

    def start_listening(self):
        self._server.start()
        
    def stop_listening(self, grace=0):
        self._server.stop(grace)

    def get_server(self):
        return self._server


# Within Greeter class(), kindly uncomment one and comment the rest of SayHello function definitions - for normal gRPC and asyncio-gRPC service testing.

if __name__ == '__main__':
    ip_addr, port = 'localhost', 50061    
    key=open('../certificate_store/server.key').read()
    crt=open('../certificate_store/server.crt').read()
    root_crt = open('../certificate_store/CA.crt').read()

    s=grpcServerAPI(max_workers=1)
    s.create_server(ip_addr, port)
    s.add_grpc_service(grpc_service_file=helloworld_echo_service_pb2, grpc_server_class=Greeter)
    s.start_listening()

    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        print("Ctrl+C captured.. Terminating program")
        s.stop_listening(0)

    