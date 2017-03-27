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

""" Python implementation of GRPC-based client -- both insecure and SSL based."""

from __future__ import print_function
from grpc.beta.interfaces import ChannelConnectivity

import grpc
import time
import random, json

import helloworld_echo_service_pb2
import helloworld_echo_service_pb2_grpc

_ONE_DAY_IN_SECONDS = 60*60*24

def cb(future):
    """ Callback extract the result of Future, or exception received by future. """
    try:
        future.result()
        print("Client message received", json.loads(future.result().message))
    except Exception as ex:
        print(future.exception())
    
def connection_callback(value):
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

        
def run_client():
    """HTTP2-based gRPC client """
    #ip_addr, port = '[::1]', 50061                     # IPv6 address shall be provided in square bracket.
    ip_addr, port = 'localhost', 50061
    addr = ip_addr + ':' + str(port)

    channel = grpc.insecure_channel(addr)
    channel.subscribe(connection_callback)              # Executes callback on change in channel's connectivity status.     
    stub = helloworld_echo_service_pb2.GreeterStub(channel)
    
    #for it in range(0,20):

    msg1= json.dumps({'request':'retrieval', 'type':'Host', 'query_parameters':['Local_FQDN', 'Remote_FQDN', 'Direction'], 'query_value':['hassaan.aalto.fi', 'hammad.aalto.fi', 'Outbound']})
    msg1= json.dumps({'request':'retrieval', 'type':'CES', 'query_parameters':['Trans_Protocol', 'Link_Alias', 'Direction', 'CES_FQDN'], 'query_value':['tcp', 'ISP-TO-ISP', 'Outbound', 'aalto.fi']})
    msg= json.dumps({'request':'retrieval', 'type':'Firewall', 'query_parameters':['fqdn'], 'query_value':['hassaan.aalto.fi']})
    msg1= json.dumps([['Firewall'], ['msisdn'], ['+04656029592']])

    print("Client sent: ", msg)
    response_future = stub.SayHello.future(helloworld_echo_service_pb2.HelloRequest(name=msg))      # Possible to add timeout parameter to gRPC future instance.
    response_future.add_done_callback(cb)
    #time.sleep(0.1)
    
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)         # To prevent termination of client program prior to receiving all messages.
    except KeyboardInterrupt:
        print('\nClient instance terminates')



if __name__ == '__main__':
    run_client()
    #run_secure_client()
