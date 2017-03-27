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
from reportlab.platypus.paraparser import _num
from pip._vendor.requests.packages.urllib3._collections import _Null
from grpc._credential_composition import channel

""" Python implementation of GRPC-based client -- both insecure and SSL based."""

from grpc.beta.interfaces import ChannelConnectivity

import grpc
import time
import random
import json
import sys

import helloworld_echo_service_pb2
#import helloworld_echo_service_pb2_grpc

from grpc.framework.common import cardinality
from grpc.framework.interfaces.face import utilities as face_utilities

_ONE_DAY_IN_SECONDS     = 60*60*24
_NUM_of_MESSAGES        = 3
_MESSAGE_SIZE           = 800


class GreeterStub(object):
  """The greeting service definition.
  """

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.SayHello = channel.unary_unary(
        '/helloworld.Greeter/SayHello',
        request_serializer=helloworld__echo__service__pb2.HelloRequest.SerializeToString,
        response_deserializer=helloworld__echo__service__pb2.HelloReply.FromString,
        )


class ClientConnections:
    def __init__(self):
        #ip_addr, port = '[::1]', 50061                     # IPv6 address shall be provided in square bracket.
        self.ip_addr, self.port = 'localhost', 50061
        self.addr = self.ip_addr + ':' + str(self.port)
        self.msg_cnt = 0
        #print("Num-of-messages '{}', message-size '{}'".format(_NUM_of_MESSAGES, _MESSAGE_SIZE))
    
    def connection_callback(self, value):
        if value==ChannelConnectivity.IDLE:
            print("Channel is IDLE")
        elif value==ChannelConnectivity.CONNECTING:
            print("gRPC client is Connecting")
        elif value==ChannelConnectivity.READY:
            print("gRPC client is Ready to send messages")
        elif value==ChannelConnectivity.SHUTDOWN:                       # This channel SHUTDOWN status is never reported by gRPC, which is a problem.
            print("Channel closed")
        elif value==ChannelConnectivity.TRANSIENT_FAILURE:
            print("Channel is facing temporary connectivity issues")
        else:
            print(value)

    def generate_random_string(self, message_size=10):
        lis=list('ascii_lowercase')
        st=''.join(random.choice(lis) for _ in range(message_size))
        return st

    def cb(self, future):
        """ Callback extract the result of Future, or exception received by future. """
        try:
            res = future.result()
            res = res.message
            print(res)
        except Exception as ex:
            print(future.exception())

    def run_client(self):
        """HTTP2-based gRPC client """    
        print("Starting un-secure gRPC Client ")
        self.channel = grpc.insecure_channel(self.addr)
        self.stub = helloworld_echo_service_pb2.GreeterStub(self.channel)
        msg = self.generate_random_string(message_size=_MESSAGE_SIZE)
        
        self._start_time=time.time()
        
        for it in range(0,_NUM_of_MESSAGES):
            response_future = self.stub.SayHello.future(helloworld_echo_service_pb2.HelloRequest(name=msg))      # Possible to add timeout parameter to gRPC future instance.
            response_future.add_done_callback(self.cb)
            
        time.sleep(4)
        self.channel.shutdown()

        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)         # To prevent termination of client program prior to receiving all messages.
        except KeyboardInterrupt:
            print('\nClient instance terminates')


    def run_secure_client(self):
        """ SSL & HTTP2-based gRPC client """
        try:
            key = open('certificate_store/client1.key').read()
            crt = open('certificate_store/client1.crt').read()
            root_crt = open('certificate_store/CA.crt').read()    
            
            self.client_credentials = grpc.ssl_channel_credentials(root_certificates=root_crt.encode(), private_key=key.encode(), certificate_chain=crt.encode())
            self.channel = grpc.secure_channel(self.addr, self.client_credentials)
            self.stub = helloworld_echo_service_pb2.GreeterStub(self.channel)
    
            msg_lst = {}
            for it in range(0, _NUM_of_MESSAGES):
                msg = self.generate_random_string(message_size=_MESSAGE_SIZE)
                msg_lst[it]=msg
            
            self._start_time=time.time()
            print("Benchmarking of secure gRPC begins now @: ", self._start_time)
            
            for it in range(0, _NUM_of_MESSAGES):
                response_future = self.stub.SayHello.future(helloworld_echo_service_pb2.HelloRequest(name=msg_lst[it]))
                response_future.add_done_callback(self.cb)
            
        except Exception as msg:
            print("Exception: ", msg)
            
        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            print('\nClient instance terminates')



if __name__ == '__main__':
    c = ClientConnections()
    c.run_client()
    #c.run_secure_client()
