"""
 Copyright (c) 2023. ByteDance Inc. All rights reserved.
A simple topo:

client-----left switch-----rightswitch----... server nodes

This script use sub process to start up nodes. You may also directly run with xterm or screen.
"""
from time import sleep

from mininet.net import Mininet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel, warning
import shlex
import os

server_num = 2  # data nodes number
curr_dir = os.getcwd()
"""
we assume  curr_dir/ has following struct
   ./bin/servertest datanode program
   ./MPDtest download program
   ./config/upnode_mn0.json config file for data node 0
   ./config/upnode_mn1.json config file for data node 1
   ./config/...........more data nodes
   ./config/downnode_mn.json download node config file
"""

if __name__ == '__main__':
    setLogLevel('info')
    net = Mininet(controller=Controller)
    info('*** Adding controller\n')
    net.addController('c0')

    server_nodes = []
    info(f'****Add {server_num} server nodes')
    for i in range(server_num):
        s = net.addHost(name=f'server{i}', ip=f'10.100.2.{i}')
        server_nodes.append(s)

    info(f'****Add 1 client node')
    client_node = net.addHost(name='client', ip='10.100.0.1')

    info('*** Adding switches\n')
    leftswitch = net.addSwitch('s1')
    rightswitch = net.addSwitch('s2')
    switchid = 3

    """
    It's not a good idea to set both bw and loss rate at same link.
    We have to split the link and set bw and loss rate separately
    """


    def addTClink(l, r, delay='5ms', bw=100):
        bridgeswitch = net.addSwitch(name=f's{switchid}')
        net.addLink(l, bridgeswitch, cls=TCLink, delay=delay, loss=1)
        net.addLink(bridgeswitch, r, cls=TCLink, bw=bw, max_queue_size=100)


    info('*** Creating links\n')
    """ Note: don't limit the bw for a very small number, like 1Kbps.
        Normally, the value of bw should be around 1Mbps to 1Gbps
    """
    # connect client node with leftside switch
    net.addLink(client_node, leftswitch, cls=TCLink, bw=100)

    serverdelays = ['20ms', '30ms', '40ms', '25ms', '30ms', '35ms', '40ms', '45ms', '50ms', '55ms']
    serverbws = [5, 5, 100, 10, 10, 10, 10, 10, 10, 10]  # Mbits

    # connect datanodes with right side switch, note we use tclink here
    for i in range(server_num):
        addTClink(rightswitch, server_nodes[i], delay=serverdelays[i], bw=serverbws[i])
        switchid += 1

    # connect left and right switches with a bottlenect link, config isdelay 25ms, bw 10Mbps
    addTClink(leftswitch, rightswitch, delay='25ms', bw=10)
    switchid += 1

    info('*** Starting network\n')
    net.start()
    info('*** Testing connectivity\n')
    # net.ping(server_nodes + [tracker_node] + [client_node])

    info('start servers\n')
    # Start servertest program in data node, redirect the stdout to server{i}stdout
    server_proc_ls = []
    server_std_f_ls = []
    for i in range(server_num):
        # use absolute dir to start servertest program, and the parameter is the upnode config json file
        server_start_cmd = f'{curr_dir}/bin/servertest {curr_dir}/config/upnode_mn{i}.json '
        server_std_f = open(f'server{i}stdout', mode='w')
        server_std_f_ls.append(server_std_f)
        server_start_cmd_args = shlex.split(server_start_cmd)
        server_proc = server_nodes[i].popen(server_start_cmd_args, stdout=server_std_f)  # no output
        server_proc_ls.append(server_proc)
        # sleep(1)  # pause for one second
    sleep(3)  # wait for three seconds to finish the connecting process

    info('*** Running CLI\n')
    warning('TYPE exit or CTRL + D to exit!! DO NOT kill the CLI interface.There will be zombie process ')

    CLI(net)  # start cmd line interface
    """
    To start download test, in CMD line interface,type: 

    client {absolute dir}/MPDtest {absolute_dir}/downnode_mn.json
    
    Once the program is successfully stopped, you may find MPDTrace.txt inside the dir of this python file.
    """

    info('*** Stopping network\n')
    # client_pcap.terminate()
    # clean server first
    for server_proc in server_proc_ls:
        server_proc.kill()
    for sf in server_std_f_ls:
        if not sf.closed:
            sf.close()

    sleep(1)
    net.stop()
