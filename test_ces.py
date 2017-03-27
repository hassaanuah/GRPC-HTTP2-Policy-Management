
#!/usr/bin/python3

import asyncio
import yaml
import configparser
import dns
import logging
import signal
import sys
import traceback
import json
import functools

import dns
import dns.message
import dns.zone

from dns.exception import DNSException
from dns.rdataclass import *
from dns.rdatatype import *

import grpc
import time
import random

import helloworld_echo_service_pb2



LOGLEVELCES = logging.WARNING

class RetCodes(object):
    POLICY_OK  = 0
    POLICY_NOK = 1
    
    AP_AVAILABLE = 0
    AP_DEPLETED = 1
    
    DNS_NOERROR  = 0    # DNS Query completed successfully
    DNS_FORMERR  = 1    # DNS Query Format Error
    DNS_SERVFAIL = 2    # Server failed to complete the DNS request
    DNS_NXDOMAIN = 3    # Domain name does not exist. For help resolving this error, read
    DNS_NOTIMP   = 4    # Function not implemented
    DNS_REFUSED  = 5    # The server refused to answer for the query
    DNS_YXDOMAIN = 6    # Name that should not exist, does exist
    DNS_XRRSET   = 7    # RRset that should not exist, does exist
    DNS_NOTAUTH  = 8    # Server not authoritative for the zone
    DNS_NOTZONE  = 9    # Name not in zone



def trace():
    print('Exception in user code:')
    print('-' * 60)
    traceback.print_exc(file=sys.stdout)
    print('-' * 60)


class CETPClientQueue:
    def __init__(self):
        self.q = asyncio.Queue()
    
    def populate_queue(self, naptr_resp):
        self.q.put(naptr_resp)
        
    async def consume_queue(self):
        while True:
            item = await self.q.get()
            print(item)
    

class CETPClient:
    def __init__(self, addr, port, loop):
        self.q = asyncio.Queue()
        self.ip_addr    = addr
        self.port       = port
        self.loop       = loop
        self.count      = 0                 # Test counter
        self.create_cetp_channel()
        self.loop.create_task(self.consume_queue())
        
    async def populate_queue(self, naptr_resp):
        print("In populate_queue() message")
        await self.q.put(naptr_resp)
        #print(self.q)
        self.loop.create_task(self.consume_queue())
        
    async def consume_queue(self):
        try:
            item = await self.q.get()
            #print("Extracted item from asyncio queue", item)
            self.q.task_done()
            self.process_message(item)
        except Exception as msg:
            print("consume_queue exception: ", msg)
        
    def create_cetp_channel(self):
        self.addr = self.ip_addr+':'+str(self.port)
        self.channel = grpc.insecure_channel(self.addr)          # Can this be put to yield? Has to be part of a separate class?
        self.stub = helloworld_echo_service_pb2.GreeterStub(self.channel)
        
    def process_message(self, msg):
        print("Message to process", msg)
        msg_to_send = "Hello, Message-1"
        msg = self.generate_cetp_message(msg_to_send)
        self.send_msg(msg)
        
    def send_msg(self, msg):
        future = self.stub.SayHello.future(msg)
        future.add_done_callback(self.cb)

    def generate_cetp_message(self, msg, ongoing=False):
        if not ongoing:
            self.count += 1
        sst, dst = self.count, 0
        j_dict = {}
        j_dict['sst'], j_dict['dst'], j_dict['msg'] = sst, dst, msg
        cetp_msg = json.dumps(j_dict)
        msg=helloworld_echo_service_pb2.HelloRequest(name=cetp_msg)
        return msg

    def cb(self, future):
        """ Callback extract the result of Future, or exception received by future. """
        try:
            res = future.result()
            resp = res.message
            print("Response received: ", resp)
            self.process_response(resp)
        except Exception as ex:
            print(future.exception())

    def process_response(self, resp):
        """ Processing response from callback """
        j_resp = json.loads(resp)
        first_cetp_msg = "Hello, Message-1"
        sst, dst, rsp_msg = j_resp['sst'], j_resp['dst'], j_resp['msg']
        print(dst, rsp_msg)
        #'''
        if (dst==0) and (rsp_msg == first_cetp_msg):            
            print("Continue negotiation ... Create another Future and respond")
            self.continue_negotiation(j_resp)
        else:
            print("Negotiation resolved. Send DNS message")
        #'''
        
    def continue_negotiation(self, j_resp):
        new_msg = "Hello, Message-2"
        msg = self.generate_cetp_message(new_msg)
        self.send_msg(msg)


     
    
class DNSServer(asyncio.DatagramProtocol):
    def __init__(self, zone=None, cb_noerror=None, cb_nxdomain=None, cb_update=None, cache=None, loop=None):
        self._logger = logging.getLogger('DNSServer')
        self._logger.setLevel(logging.INFO)

        self._zone = zone
        self._cache = cache
        self._load_naptr_records()
        self.local_repo={}
        self.channel_repo = {}
        self.count = 0
        self.loop = loop
        
        """
        # Define standard functions for processing DNS queries
        self._cb_noerror  = self._do_process_query_noerror
        self._cb_nxdomain = self._do_process_query_nxdomain
        self._cb_update   = self._do_process_query_update

        # Define callback functions for connecting to other resolvers
        if cb_noerror:
            self._logger.info('Resolving internal records via {}'.format(
                cb_noerror))
            self._cb_noerror = cb_noerror

        if cb_nxdomain:
            self._logger.info('Resolving external records via {}'.format(
                cb_nxdomain))
            self._cb_nxdomain = cb_nxdomain
        
        if cb_update:
            self._logger.info('Resolving DNS update via {}'.format(
                cb_update))
            self._cb_update = cb_update
        """
        
    def callback_sendto(self, query, response, addr):
        """ Send response to host """
        self._logger.debug('Callback for query {}'.format(query.id))
        if response is None:
            self._send_error(query, addr, dns.rcode.REFUSED)
            return
        self._send_msg(response, addr)

    def connection_made(self, transport):
        self._transport = transport

    def datagram_received(self, data, addr):
        self._logger.debug(
            'Received data from {0}:{1} ({2} bytes) "{3}"'.format(addr[
                0], addr[1], len(data), data))
        try:
            query = dns.message.from_wire(data)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            # self._logger.warning('{}'.format(e))
            self._logger.error(
                'Failed to parse DNS message from {0}:{1} ({2} bytes) "{3}"'.format(
                    addr[0], addr[1], len(data), data))
            return
        try:
            # Process received message
            self.process_message(query, addr)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            # self._logger.warning('{}'.format(e))
            self._logger.error(
                'Failed to process DNS message from {0}:{1} ({2} bytes) "{3}"'.format(
                    addr[0], addr[1], len(data), data))
            return

  
    def _load_naptr_records(self):
        self.naptr_records = {}
        self.naptr_records['dest-id']           = ("destHost/service-id, dest-cesid,    dest-ip, dest-port, proto")
        self.naptr_records['hosta1.demo.lte.']   = ('hosta1.demo.lte', 'cesa.demo.lte', '127.0.0.1', '50061', 'tcp')
        self.naptr_records['hosta2.demo.lte.']   = ('hosta2.demo.lte', 'cesa.demo.lte', '127.0.0.1', '50061', 'tcp')
        self.naptr_records['hosta3.demo.lte.']   = ('hosta3.demo.lte', 'cesa.demo.lte', '127.0.0.1', '50061', 'tcp')
        self.naptr_records['hosta4.demo.lte.']   = ('hosta4.demo.lte', 'cesa.demo.lte', '127.0.0.1', '50061', 'tcp')
        self.naptr_records['hostb1.demo.lte.']   = ('hostb1.demo.lte', 'cesb.demo.lte', '127.0.0.1', '50061', 'tcp')
        self.naptr_records['hostb2.demo.lte.']   = ('hostb2.demo.lte', 'cesb.demo.lte', '127.0.0.1', '49001', 'tcp')
        self.naptr_records['hostb3.demo.lte.']   = ('hostb3.demo.lte', 'cesb.demo.lte', '127.0.0.1', '49002', 'tcp')
        self.naptr_records['hostb4.demo.lte.']   = ('hostb4.demo.lte', 'cesb.demo.lte', '127.0.0.1', '49002', 'tcp')
        self.naptr_records['hostb5.demo.lte.']   = ('hostb5.demo.lte', 'cesb.demo.lte', '127.0.0.1', '49003', 'tls')
        self.naptr_records['hostb6.demo.lte.']   = ('hostb6.demo.lte', 'cesb.demo.lte', '127.0.0.1', '49003', 'tls')
        self.naptr_records['hostc1.demo.lte.']   = ('hostc1.demo.lte', 'cesc.demo.lte', '127.0.0.3', '49001', 'tcp')
        self.naptr_records['hostc2.demo.lte.']   = ('hostc2.demo.lte', 'cesc.demo.lte', '127.0.0.3', '49001', 'tcp')
        self.naptr_records['www.google.com.']    = ('www.google.com',  'cesd.demo.lte', '127.0.0.4', '49001', 'tcp')
        self.naptr_records['www.aalto.fi.']      = ('www.aalto.fi',    'cese.demo.lte', '127.0.0.5', '49001', 'tcp')
        
    
    def resolve_naptr(self, domain):
        search_domain = str(domain)
        print("Resolving DNS NAPTR for domain: ", search_domain)
        if search_domain in self.naptr_records:
            return self.naptr_records[search_domain]
        else:
            print("Domain names doesn't exist.. Returning the default result")
            default_dns_rec = (search_domain, 'cesb.demo.lte', '127.0.0.1', '50061', 'tcp')
            return default_dns_rec

    def generate_cetp_message(self, msg=''):
        self.count += 1
        sst, dst = self.count, 0
        msg = "Hello, Message-1"
        j_dict = {}
        j_dict['sst'], j_dict['dst'], j_dict['msg'] = sst, dst, msg
        cetp_msg = json.dumps(j_dict)
        msg=helloworld_echo_service_pb2.HelloRequest(name=cetp_msg)
        return msg

    def process_response(self, resp):
        """ Processing response from callback """
        rsp = json.loads(resp)
        first_cetp_msg = "Hello, Message-1"
        if rsp['msg'] == first_cetp_msg:
            print("Create another Future and respond")
        else:
            print("Future methodology worked. Send DNS response")

    
    def cb(self, future):
        """ Callback extract the result of Future, or exception received by future. """
        try:
            res = future.result()
            resp = res.message
            print("Response received: ", resp)
            self.process_response(resp)
        except Exception as ex:
            print(future.exception())


    def create_cetp_channel(self, ip_addr, port):
        addr = ip_addr+':'+str(port)
        channel = grpc.insecure_channel(addr)          # Can this be put to yield? Has to be part of a separate class?
        stub = helloworld_echo_service_pb2.GreeterStub(channel)
        self.channel_repo[addr] = (channel, stub)
        return (channel, stub)

    def pass_to_cetp_client(self, r_ip, r_port, query):
        """ Idea is to pass query to appropriate client instance 
        
        Challenge is creation of that client instance - without blocking DNS module
        Storing and locating that client instance.
        """
        
        if (r_ip, r_port) in self.channel_repo:
            print("Reusing an existing channel")
            client = self.channel_repo[(r_ip, r_port)]
        else:
            print("Creating new channels")
            client = CETPClient(r_ip, r_port, self.loop)
        
        self.loop.create_task(client.populate_queue(query))
        
    
    def process_message(self, query, addr):
        """ Process a DNS message received by the DNS Server """
        """ I trigger CETP resolution here, and all the other CETP Manager magic for client side """

        q = query.question[0]
        name, rdtype, rdclass = q.name, q.rdtype, q.rdclass
        opcode = query.opcode()
        key = (query.id, name, rdtype, rdclass, addr)
        
        print("Received DNS query for '%s'" % str(name))
        dest_id, r_cesid, r_ip, r_port, r_transport = self.resolve_naptr(name)
        
        self.pass_to_cetp_client(r_ip, r_port, query)
        
        
        '''
        cb_args = (query, addr)
        if not self._cetpManager.has_local_endpoint(remote_cesid=r_cesid, remote_ip=r_ip, remote_port= r_port, remote_transport=r_transport):
            local_ep=self._cetpManager.create_local_endpoint(remote_cesid=r_cesid, remote_ip=r_ip, remote_port= r_port, remote_transport=r_transport, dest_hostid=dest_id, cb_func=self.process_dns_query_callback, cb_args=cb_args)
        else:
            local_ep = self._cetpManager.get_local_endpoint(remote_cesid=r_cesid, remote_ip=r_ip, remote_port= r_port, remote_transport=r_transport)
            # Message produced by start_transaction() or others
            local_ep.process_message(r_cesid=r_cesid, cb_args=(query, addr))
        '''
        


    def process_dns_query_callback(self, dnsmsg, addr, success=True):
        #dns_query = dns.message.from_wire(dnsmsg)
        if success:
            resp = self._successful_cetp_resolution(dnsmsg)
        else:
            resp = self._failed_cetp_resolution(dnsmsg)
        self._transport.sendto(resp.to_wire(), addr)

    def _successful_cetp_resolution(self, dns_query):
        #For no DNS response
        qtype      = dns_query.question[0].rdtype
        domain     = dns_query.question[0].name.to_text().lower()
        flags = [dns.flags.AA, dns.flags.RA]
        
        rrset = dns.rrset.from_text(domain, 10, dns.rdataclass.IN, dns_query.question[0].rdtype, "127.0.0.1")
        dns_response = self.create_dns_response(dns_query, dns.rcode.NOERROR, flags, [rrset], authority_rr = [], additional_rr=[])
        #print(dns_response.to_text())
        return dns_response

    def _failed_cetp_resolution(self, dns_query):
        #For no DNS response
        qtype      = dns_query.question[0].rdtype
        domain     = dns_query.question[0].name.to_text().lower()
        flags = [dns.flags.AA, dns.flags.RA]
        
        dns_response = self.create_dns_response(dns_query, dns.rcode.NXDOMAIN, flags, [], authority_rr = [], additional_rr=[])
        return dns_response
    
    def create_dns_response(self, dns_query, rcode, flags, answer_rr, authority_rr = [], additional_rr = []):
        """
        Create a DNS response. 
        
        @param dns_query: The original DNS query.
        @param rcode: The DNS_RCODE of the response. 
        @param flags: A list with the flags of the response. 
        @param answer_rr: A list of rrset with the response records. 
        @param authority_rr: A list of rrset with the authority records. 
        @param additional_rr: A list of rrset with the additional records. 
        @return: The built DNS message.
        """
        #print "create_dns_response"
        dns_response = dns.message.make_response(dns_query)
        dns_response.set_rcode(rcode)
        for rr in answer_rr:
            dns_response.answer.append(rr)
        for rr in authority_rr:
            dns_response.authority.append(rr)
        for rr in additional_rr:
            dns_response.additional.append(rr)
        for flag in flags:
            dns_response.flags |= flag
        return dns_response


    def _do_process_query_cache(self, query, addr, cback):
        """ Generate DNS response with the available records from the zone """
        pass

    def _do_process_query_noerror(self, query, addr, cback):
        """ Generate DNS response with the available records from the zone """

        self._logger.debug('_do_process_query_noerror')

        q = query.question[0]
        name, rdtype, rdclass = q.name, q.rdtype, q.rdclass

        response = dns.message.make_response(query, recursion_available=True)
        rrset = self._get_rrset(name, rdtype)

        # Fill the answer section
        # Contains the available records
        if rrset:
            self._logger.info('Record found for {0}/{1}'.format(
                name, dns.rdatatype.to_text(rdtype)))
            response.set_rcode(dns.rcode.NOERROR)
            response.answer.append(rrset)

        elif rdtype == dns.rdatatype.CNAME:
            self._logger.info('Record not found for {0}/{1}'.format(
                name, dns.rdatatype.to_text(rdtype)))
            response.set_rcode(dns.rcode.NOERROR)

        elif rdtype != dns.rdatatype.CNAME:
            # Resolve CNAME records if available
            response.set_rcode(dns.rcode.NOERROR)
            cname_rrset = self._resolve_cname(name, rdtype)
            response.answer = cname_rrset
            self._logger.info(
                'Found {0} related records for {1} via {2}'.format(
                    len(cname_rrset), name, dns.rdatatype.to_text(
                        dns.rdatatype.CNAME)))

        # Fill authority section
        # Contains the NS records
        ns_rrset = self._get_rrset(self._zone.origin, dns.rdatatype.NS)
        #ns_rrset = self._zone.get_rrset(self._zone.origin, dns.rdatatype.NS)
        response.authority.append(ns_rrset)

        # Fill additional section
        # Contains the A / AAAA records for the NS
        for rr in ns_rrset:
            for rr_type in [dns.rdatatype.A, dns.rdatatype.AAAA]:
                ip_rrset = self._get_rrset(rr.target, rr_type)
                if ip_rrset:
                    response.additional.append(ip_rrset)

        response.flags |= dns.flags.AA

        # Use cback function to send ready-made response
        cback(query, response, addr)

    def _do_process_query_nxdomain(self, query, addr, cback):
        """ Generate None DNS response """
        self._logger.debug('_do_process_query_nxdomain')
        cback(query, None, addr)
    
    def _do_process_query_update(self, query, addr, cback):
        """ Generate NoError DNS response """
        self._logger.warning('_do_process_query_update')
        # Send generic DNS Response NOERROR
        response = dns.message.make_response(query)
        self._logger.debug('Sent DDNS response to {}:{}'.format(addr[0],addr[1]))
        cback(query, response, addr)
            
    def _get_cache(self, addr, name, rdtype, rdclass):
        """ Return a cached response """
        return self._cache.get((name, rdtype, rdclass))

    def _get_node(self, name):
        """ Return a node of the DNS zone """
        return self._zone.get_node(name)

    def _get_rrset(self, name, rdtype):
        """ Return the records of the node in the DNS zone """
        return self._zone.get_rrset(name, rdtype)

    def _name_in_zone(self, name):
        """ Return True if the name belongs to the zone """
        return name.is_subdomain(self._zone.origin)

    def _name_in_cache(self, addr, name, rdtype, rdclass):
        """ Return True if the record exists in the cache """
        #Cached is not enabled
        if not self._cache:
            return False

        return (self._cache.get((name, rdtype, rdclass)) is not None)

    def _resolve_cname(self, name, rdtype):
        """ Resolve 1 level of indirection for CNAME records """
        cname_rrset = self._get_rrset(name, dns.rdatatype.CNAME)
        rrset = []
        # Resolve 1 level of indirection for CNAME
        # TODO: Make recursive and yield all records
        if not cname_rrset:
            return rrset

        # Add CNAME records to list
        rrset.append(cname_rrset)

        # Resolve CNAME records from DNS zone
        for rr in cname_rrset:
            ip_rrset = self._get_rrset(rr.target, rdtype)
            if ip_rrset:
                rrset.append(ip_rrset)

        return rrset

    def _send_msg(self, dnsmsg, addr):
        q = dnsmsg.question[0]
        self._logger.debug('Send message {0} {1}/{2} {3} to {4}{5}'.format(
            dnsmsg.id, q.name.to_text(), dns.rdatatype.to_text(q.rdtype),
            dns.rcode.to_text(dnsmsg.rcode()), addr[0], addr[1]))

        self._transport.sendto(dnsmsg.to_wire(), addr)

    def _send_error(self, query, addr, rcode):
        response = dns.message.make_response(query, recursion_available=True)
        response.set_rcode(rcode)
        self._send_msg(response, addr)

    
class CustomerEdgeSwitch(object):
    def __init__(self, name='CustomerEdgeSwitch', async_loop = None, config_file=None):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(LOGLEVELCES)
        
        # Get event loop
        self._loop = async_loop
        self._read_configuration(config_file)
        
        # Enable debugging
        #self._set_verbose()
        
        # Capture signals
        self._capture_signal()
        
        # Initialize CETP
        #self._init_cetp()

        # Initialize DNS
        self._init_dns()
        
    
    def _capture_signal(self):
        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(getattr(signal, signame), functools.partial(self._signal_handler, signame))
        
    
    def _read_configuration(self, filename):
        config_file = open(filename)
        self.ces_conf = yaml.load(config_file)
        
    def _init_cetp(self):
        """ Initiate CETP manager... Manages CETPLocalManager() and CETPServers() """
        self.ces_details     = self.ces_conf['CESIdentification']
        self.ces_name        = self.ces_details['name']
        self.cesid           = self.ces_details['domainId']
        self.ces_certificate = self.ces_details['certificate']
        self.ces_privatekey  = self.ces_details['private_key']
        self.ca_certificate  = self.ces_details['ca_certificate']                     # Could be a list of popular/trusted (certificate issuing) CA's certificates

        self._host_policies = self.ces_conf["cetp_policy_file"]
        self.cetp_mgr = cetpManager.CETPManager(self._host_policies, self.cesid, self.ces_certificate, self.ces_privatekey, self.ca_certificate, loop=self._loop)
        cetp_server_list = self.ces_conf["CETPServers"]["serverNames"]
        for srv in cetp_server_list:
            srv_info = self.ces_conf["CETPServers"][srv]
            srv_addr, srv_port, srv_proto = srv_info["ip"], srv_info["port"], srv_info["transport"]
            self.cetp_mgr.create_server_endpoint(srv_addr, srv_port, srv_proto)

        
    def _init_dns(self):
        # Store all DNS related parameters in a dictionary
        self._dns = {}
        self._dns['addr'] = {}
        self._dns['node'] = {}
        self._dns['activequeries'] = {}
        self._dns['soa'] = self.ces_conf['DNS']['soa']
        self._dns['timeouts'] = self.ces_conf['DNS']['timeouts']
        
        # Create DNS Zone file to be populated
        #self._dns['zone'] = cesdns.load_zone(self.ces_conf['DNS']['zonefile'], self.ces_conf['DNS']['soa'])
        
        self._logger.warning('DNS rtx-timeout for CES NAPTR: {}'.format(self._dns['timeouts']['naptr']))
        self._logger.warning('DNS rtx-timeout for CES A:     {}'.format(self._dns['timeouts']['a']))
        self._logger.warning('DNS rtx-timeout for other:     {}'.format(self._dns['timeouts']['any']))
        
        # Get address tuple configuration of DNS servers
        for k, v in self.ces_conf['DNS']['server'].items():
            self._dns['addr'][k] = (v['ip'], v['port'])
        
        # Initiate specific DNS servers
        self._init_dns_loopback()
        
    def _init_dns_loopback(self):
        # Initiate DNS Server in Loopback
        #zone = self._dns['zone']
        addr = self._dns['addr']['loopback']
        self._logger.warning('Creating DNS Server {} @{}:{}'.format('loopback', addr[0],addr[1]))
        
        # Define callbacks for different DNS queries
        cb_noerror = None
        cb_nxdomain = self.process_dns_query
        cb_udpate = self.process_dns_update
        
        factory = DNSServer(cb_noerror=cb_noerror, cb_nxdomain=cb_nxdomain, cb_update=cb_udpate, loop=self._loop)
        self._dns['node']['loopback'] = factory
        self._loop.create_task(self._loop.create_datagram_endpoint(lambda: factory, local_addr=addr))
    
    def _init_datarepository(self):
        self._logger.warning('Initializing data repository')
        self._udr = self.ces_conf['DATAREPOSITORY']
        self._init_userdata(self._udr['userdata'])
    
    def _init_userdata(self, filename):
        self._logger.warning('Initializing user data')
        
        data = self._load_configuration(filename)
        for k, v in data['HOSTS'].items():
            self._logger.warning('Registering host {}'.format(k))
            ipaddr = data['HOSTS'][k]['ipv4']
            self.register_user(k, 1, ipaddr)
            
    def _set_verbose(self):
        self._logger.warning('Enabling logging.DEBUG')
        logging.basicConfig(level=logging.DEBUG)
        self._loop.set_debug(True)
        
    def _signal_handler(self, signame):
        self._logger.critical('Got signal %s: exit' % signame)
        try:
            print("Must close all DNS and CETP sockets for clients and Servers. AND End the loop.")
            for k,v in self.ces_conf['CETPServers'].items():
                print("MUST terminate CETP server")
                #addr = self._dns['addr'][k]
                #self._logger.warning('Terminating DNS Server {} @{}:{}'.format(k, addr[0],addr[1]))
                #v.connection_lost(None)
        except:
            trace()
        finally:
            self._loop.stop()
    
    def begin(self):
        print('CESv2 will start now...')
        self._loop.run_forever()
        
    ############################################################################
    ######################  POLICY PROCESSING FUNCTIONS  #######################
    
    def _process_local_policy(self, src_policy, dst_policy):
        self._logger.warning('Processing local policy for {} -> {}'.format(src_policy, dst_policy))
        if src_policy is dst_policy:
            return (RetCodes.POLICY_OK, True)
        else:
            return (RetCodes.POLICY_NOK, False)
    
    def create_local_connection(self, src_host, dst_service):
        self._logger.warning('Connecting host {} to service {}'.format(src_host, dst_service))
        import random
        # Randomize policy check
        retCode = self._process_local_policy(1, random.randint(0,1))
        if retCode[0] is RetCodes.POLICY_NOK:
            self._logger.warning('Failed to match policy!')
            return (RetCodes.DNS_NXDOMAIN, 'PolicyMismatch')
        
        try:
            self._logger.warning('Policy matched! Create a connection')
            ap = self._addresspoolcontainer.get('proxypool')
            ipaddr = ap.allocate(src_host)
            
            d = {'src':'192.168.0.50','psrc':'172.16.0.0',
                 'dst':'192.168.0.50','pdst':'172.16.0.1'}
            connection = network.ConnectionCESLocal(**d)
            self._network.create_connection(connection)
            
            return (RetCodes.DNS_NOERROR, ipaddr)
        except KeyError:
            self._logger.warning('Failed to allocate proxy address for host')
            return (RetCodes.DNS_REFUSED, 'PoolDepleted')
    
    
    ############################################################################
    ########################  DNS PROCESSING FUNCTIONS  ########################
    
    def process_dns_query(self, query, addr, cback):
        """ Perform public DNS resolutions of a query """
        q = query.question[0]
        key = (query.id, q.name, q.rdtype, q.rdclass, addr, cback)
        
        self._logger.warning('Resolve query {0} {1}/{2} from {3}:{4}'.format(query.id, q.name.to_text(), dns.rdatatype.to_text(q.rdtype), addr[0], addr[1]))
        
        if key in self._dns['activequeries']:
            # Continue ongoing resolution
            (resolver, query) = self._dns['activequeries'][key]
            resolver.process_query(query, addr)
        else:
            # Resolve DNS query as is
            self._do_resolve_dns_query(query, addr, cback)

    def process_dns_query_lan_noerror(self, query, addr, cback):
        """ Process DNS query from private network of an existing host """
        q = query.question[0]
        key = (query.id, q.name, q.rdtype, q.rdclass, addr, cback)

        if (is_ipv4(addr[0]) and q.rdtype == dns.rdatatype.A) or \
        (is_ipv6(addr[0]) and q.rdtype == dns.rdatatype.AAAA):
            self._logger.warning('Resolve local CES policy')
            retCode = self.create_local_connection(addr[0], q.name)
            if retCode[0] is RetCodes.DNS_NOERROR:
                response = cesdns.make_response_answer_rr(query, q.name, q.rdtype, retCode[1])
            else:
                response = cesdns.make_response_rcode(query, retCode[0])
            cback(query, response, addr)
        else:
            # Resolve DNS query as is
            self._do_resolve_dns_query(query, addr, cback)
    
    def process_dns_query_lan_nxdomain(self, query, addr, cback):
        """ Process DNS query from private network of a non existing host """
        q = query.question[0]
        key = (query.id, q.name, q.rdtype, q.rdclass, addr, cback)
        
        self._logger.warning('Resolve query for CES discovery {0} {1}/{2} from {3}:{4}'.format(query.id, q.name.to_text(), dns.rdatatype.to_text(q.rdtype), addr[0], addr[1]))
        
        if key in self._dns['activequeries']:
            # Continue ongoing resolution
            (resolver, query) = self._dns['activequeries'][key]
            resolver.process_query(query, addr)
        elif is_ipv4(addr[0]) and q.rdtype == dns.rdatatype.A:
            # Resolve DNS query for CES IPv4
            timeouts = dict(self._dns['timeouts'])
            resolver = cesdns.DNSResolverCESIPv4(self._loop, query, addr, self._dns['addr']['resolver'],self._do_resolver_callback, key, timeouts=timeouts)
            self._dns['activequeries'][key] = (resolver, query)
            resolver.begin()
        elif is_ipv6(addr[0]) and q.rdtype == dns.rdatatype.AAAA:
            # Resolve DNS query for CES IPv6
            timeouts = dict(self._dns['timeouts'])
            resolver = cesdns.DNSResolverCESIPv6(self._loop, query, addr, self._dns['addr']['resolver'],self._do_resolver_callback, key, timeouts=timeouts)
            self._dns['activequeries'][key] = (resolver, query)
            resolver.begin()
        else:
            # Resolve DNS query as is
            self._do_resolve_dns_query(query, addr, cback)
            
    def process_dns_query_wan_noerror(self, query, addr, cback):
        """ Process DNS query from public Internet of an existing host """
        q = query.question[0]
        key = (query.id, q.name, q.rdtype, q.rdclass, addr, cback)
        
        # Serve public records...
        
        ## Filter NAPTR and A records
        cback(query, None, addr)
    
    def process_dns_query_wan_nxdomain(self, query, addr, cback):
        """ Process DNS query from public Internet of a non existing host """
        q = query.question[0]
        key = (query.id, q.name, q.rdtype, q.rdclass, addr, cback)
        
        # We have received a query for a domain that should exist in our zone but it doesn't
        cback(query, None, addr)
    
    def process_dns_update(self, query, addr, cback):
        """ Generate NoError DNS response """
        self._logger.debug('process_update')
        
        try:
            rr_a = None
            #Filter hostname and operation
            for rr in query.authority:
                #Filter out non A record types
                if rr.rdtype == dns.rdatatype.A:
                    rr_a = rr
                    break
            
            if not rr_a:
                # isc-dhcp-server uses additional TXT records -> don't process
                self._logger.debug('Failed to find an A record')
                return
            
            name_str = rr_a.name.to_text()
            if rr_a.ttl:
                self.register_user(name_str, rr_a.rdtype, rr_a[0].address)
            else:
                self.deregister_user(name_str, rr_a.rdtype, rr_a[0].address)
                
        except Exception as e:
            self._logger.error('Failed to process UPDATE DNS message')
            trace()
        finally:
            # Send generic DDNS Response NOERROR
            response = cesdns.make_response_rcode(query, RetCodes.DNS_NOERROR)
            self._logger.debug('Sent DDNS response to {}:{}'.format(addr[0],addr[1]))
            cback(query, response, addr)
            
    def _do_resolver_callback(self, metadata, response=None):
        try:
            (queryid, name, rdtype, rdclass, addr, cback) = metadata
            (resolver, query) = self._dns['activequeries'].pop(metadata)
        except KeyError:
            self._logger.warning('Query has already been processed {0} {1}/{2} from {3}:{4}'.format(queryid, name, dns.rdatatype.to_text(rdtype), addr[0], addr[1]))
            return

        if response is None:
            self._logger.warning(
                'This seems a good place to create negative caching...')

        # Callback to send response to host
        cback(query, response, addr)

    def _do_resolve_dns_query(self, query, addr, cback):
        q = query.question[0]
        key = (query.id, q.name, q.rdtype, q.rdclass, addr, cback)

        self._logger.warning(
            'Resolve normal query {0} {1}/{2} from {3}:{4}'.format(
                query.id, q.name.to_text(), dns.rdatatype.to_text(
                    q.rdtype), addr[0], addr[1]))

        # This DNS resolution does not require any kind of mangling
        timeouts = list(self._dns['timeouts']['any'])
        resolver = ResolverWorker(self._loop, query, self._do_resolver_callback,
                                   key, timeouts)
        self._dns['activequeries'][key] = (resolver, query)
        raddr = self._dns['addr']['resolver']
        self._loop.create_task(self._loop.create_datagram_endpoint(lambda: resolver, remote_addr=raddr))
    
    
    
    
if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        ces = CustomerEdgeSwitch(async_loop = loop, config_file = sys.argv[1])
        ces.begin()
    except Exception as e:
        print(format(e))
        trace()
    finally:
        loop.close()
    print('Bye!')


