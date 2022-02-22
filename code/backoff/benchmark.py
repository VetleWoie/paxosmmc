import argparse
from matplotlib import pyplot as plt
from utils import Config
from subprocess import Popen, PIPE
import signal
import concurrent.futures as cf
from concurrent.futures import ThreadPoolExecutor
from message import RequestMessage
import requests
import pickle
import json
import time
import threading

from start_system import run_on_cluster

USER = 'vho023'
CONFIG_FILE = "./paxosconfig.json"

def execute_paxos(fault_tolerance):
    print(f"Running paxos: Replicas: {fault_tolerance+1} Leaders: {fault_tolerance+1}, Acceptors: {2*fault_tolerance+1}")
    # command = ['ssh', 'compute-0-0', f'cd "/home/vho023/3203/paxosmmc/code/backoff"; python3.10 start_system.py -f {fault_tolerance}']
    run_on_cluster(fault_tolerance=fault_tolerance, config_file=CONFIG_FILE, continous_run=False)
    # return Popen(command,)# stdin=PIPE, stdout=PIPE, stderr=PIPE)

def make_request_to_replica(replica, i):
    msg = RequestMessage(f"{replica}", f"{i}")
    pickled_msg = pickle.dumps(msg)
    r = requests.post(f"http://{replica}/{len(pickled_msg)}", pickled_msg)

def check_replicas(config, num_requests):
    print("Checking replicas for correctnes")
    done = False
    while not done:
        response = []
        for replica in config.replicas:
            r = requests.get(f"http://{replica}")
            response.append(json.loads(r.text))
        for rep1, rep2 in zip(response[:-1], response[1:]):
            # print("Checking responses agianst each other")
            # print(len(rep1),rep1)
            # print(len(rep2),rep2)
            if len(rep1) >= num_requests and rep1 == rep2:
                # print("Everything looks correct")
                done=True
            if rep1 != rep2:
                done=False
                # print("Responses differ")
            if len(rep1) < num_requests:
                done=False
                # print(f"Not all values are present {num_requests} != {len(rep1)}")
    print("Responses are correct!")
        
def run_requests(num_requests,config):
    with ThreadPoolExecutor() as pool:
        tasks = []
        print(f"Submitting {num_requests} requests concurrently")
        t1 = time.time_ns()
        for i in range(num_requests):
            tasks.append(pool.submit(make_request_to_replica, config.replicas[i % len(config.replicas)], i))
        cf.wait(tasks)
        for task in tasks:
            result = task.result()
        tasks.append(pool.submit(check_replicas, config, num_requests))
        cf.wait(tasks)
        t2 = time.time_ns()
        for task in tasks:
            result = task.result()
        print(f"Paxos finished correctly in {(t2-t1)/10E9} s")
        pool.shutdown()
    return t2-t1


def test_throughput(start, stop, step, num_requests=10):
    sleeptime = 20
    config = Config.from_jsonfile(CONFIG_FILE)
    with open("throughput.csv",'w') as file:
        file.write(f"fault_tolerance, time_{num_requests}\n")
        for fault_tolerance in range(start, stop, step):
            print()
            print(f"Running benchmark with fault tolerance: {fault_tolerance}")
            #Start paxos instance with specified fault tolerance
            execute_paxos(fault_tolerance)
            print(f"Sleeping for {sleeptime} seconds to let it boot up")
            time.sleep(sleeptime)
            runtime = run_requests(num_requests, config)
            stop_paxos()
            file.write(f"{fault_tolerance},{runtime/10E9}\n")

def stop_paxos():
    """
    Kill all paxos servers from this paxos system on interupt signal
    """
    print("Cleaning all nodes")
    c = Config.from_jsonfile(CONFIG_FILE)
    for leader in c.leaders:
        node = leader.split(':')[0]
        command = ['ssh', node.split(':')[0], 'killall', '-u', USER]
        Popen(command,stdin=PIPE, stdout=PIPE, stderr=PIPE)
    for replica in c.replicas:
        node = replica.split(':')[0]
        command = ['ssh', node.split(':')[0], 'killall', '-u', USER]
        Popen(command,stdin=PIPE, stdout=PIPE, stderr=PIPE)
    for acceptor in c.acceptors:
        node = acceptor.split(':')[0]
        command = ['ssh', node.split(':')[0], 'killall', '-u', USER]
        Popen(command,stdin=PIPE, stdout=PIPE, stderr=PIPE)

def kill_cluster(signal,frame):
    """
    Kill all paxos servers from this paxos system on interupt signal
    """
    print("Cleaning all nodes")
    c = Config.from_jsonfile(CONFIG_FILE)
    for leader in c.leaders:
        node = leader.split(':')[0]
        command = ['ssh', node.split(':')[0], 'killall', '-u', USER]
        Popen(command,stdin=PIPE, stdout=PIPE, stderr=PIPE)
    for replica in c.replicas:
        node = replica.split(':')[0]
        command = ['ssh', node.split(':')[0], 'killall', '-u', USER]
        Popen(command,stdin=PIPE, stdout=PIPE, stderr=PIPE)
    for acceptor in c.acceptors:
        node = acceptor.split(':')[0]
        command = ['ssh', node.split(':')[0], 'killall', '-u', USER]
        Popen(command,stdin=PIPE, stdout=PIPE, stderr=PIPE)
    exit(0)
    

if __name__=="__main__":
    signal.signal(signal.SIGINT, kill_cluster)
    # #Setup argparser:
    # parser = argparse.ArgumentParser(description="Benchmark script for Paxos")
    # parser.add_argument('-f', '--fault_tolerance', type=int, help="Automaticly choose number of replicas, acceptors and leaders based on needed fault tolerance")
    # parser.add_argument('-c', '--config_file', type=str, default=CONFIG_FILE, help="Path to file where start script stores configuration needed for paxos servers")
    # parser.add_argument('--folder', type=str, default="/home/vho023/3203/paxosmmc/code/backoff", help="Path to where on cluster the folder with executable is")
    # parser.add_argument('-r', '--replicas', type=int, default=None,help="Number of replicas needed, will overwrite fault tolerance")
    # parser.add_argument('-l', '--leaders', type=int, default=None,help="Number of leaders needed, will overwrite fault tolerance")
    # parser.add_argument('-a', '--acceptors', type=int, default=None,help="Number of acceptors needed, will overwrite fault tolerance")
    # parser.add_argument('-p', '--port', type=int, default=64209, help="Port hvere all paxos instance servers will be running")
    # parser.add_argument('-pr', '--port_range', type=tuple, default=(64210,64310), help="Port range where leaders can spawn new scouts and commanders")
    # parser.add_argument('--user',type=str, default='vho023', help="User which is running this paxos instance, used when shutting down processes")
    # pass
    # test_throughput(1,10,1,3)
    # config = Config.from_jsonfile(CONFIG_FILE)
    # run_requests(5, config)
    # make_request_to_replica(config.replicas[1], 1)
    # check_replicas(config, 1)
    # for i in range(10):
        # make_request_to_replica()
    test_throughput(1,39,1, num_requests=20)
    # p = execute_paxos(1)
    # while p.poll():
    #     pass
    # kill_cluster(0,0)