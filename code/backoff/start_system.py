import sys
import signal
import json
from subprocess import Popen, PIPE
from utils import Config
import time
CONFIG_FILE = "./paxosconfig.json"
USER = "vho023"

def findNodes(numNodes):
        ##Find available nodes
        with Popen(['sh','/share/apps/ifi/available-nodes.sh'], stdin=PIPE, stdout=PIPE, stderr=PIPE) as p: 
            availableNodes, err = p.communicate()
            availableNodes = availableNodes.decode().splitlines()

        if len(availableNodes) < numNodes:
            sys.stderr.write("Not enough available nodes. Availabe Nodes: " + str(len(availableNodes)) + " Got: " + str(numNodes) + "\n")
            exit()
        return availableNodes[:numNodes]

def create_config_dict(port,num_replica, num_leader, num_acceptor, path_to_file=None):
    #Find nodes to run paxos on
    config_dict = {
        "replicas": [],
        "leaders": [],
        "acceptors": []
    }
    nodes = findNodes(num_replica+num_leader+num_acceptor)

    replica_count, leader_count, acceptor_count = 0,0,0
    for node in nodes:
        if replica_count < num_replica:
            config_dict["replicas"].append(f"{node}:{port}")
            replica_count += 1
        elif leader_count < num_leader:
            config_dict["leaders"].append(f"{node}:{port}")
            leader_count += 1
        elif acceptor_count < num_acceptor:
            config_dict["acceptors"].append(f"{node}:{port}")
            acceptor_count += 1
        else:
            break

    if path_to_file is not None:
        #Create config file:
        with open(path_to_file,"w") as configfile:
            json.dump(config_dict,configfile, indent=4)
    return config_dict

def execute_replica(adress, config_file, path):
    node = adress.split(':')[0]
    command = ['ssh', node, f'cd {path}; python3.10 replica.py {adress} {config_file}']
    print(f"Running replica at: {adress}")
    return Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE)

def execute_acceptor(adress, path):
    node = adress.split(':')[0]
    command = ['ssh', node, f'cd {path}; python3.10 acceptor.py {adress}']
    print(f"Running acceptor at: {adress}")
    return Popen(command,stdin=PIPE, stdout=PIPE, stderr=PIPE)

def execute_leader(adress, available_ports, config_file, path):
    node = adress.split(':')[0]
    command = ['ssh', node, f'cd {path}; python3.10 leader.py {adress} {config_file} {available_ports[0]} {available_ports[1]}']
    print(f"Running leader at: {adress}")
    return Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE)

def run_on_cluster(port=64209,
                    available_ports=(64210,64310),
                    num_replica=None, 
                    num_leader=None, 
                    num_acceptor=None, 
                    fault_tolerance=None,
                    path_to_folder="/home/vho023/3203/paxosmmc/code/backoff", 
                    config_file=CONFIG_FILE,
                    continous_run=True):
    #User has the ability to either choose level of fault tolerance or number of replicas, leaders and acceptors directly.
    if fault_tolerance is not None:
        #From Paxos made moderatly complex paper.
        #Robbert van Renesse and Deniz Altınbüken. Paxos Made Moderately Complex. In ACM Computing Surveys. Vol. 47. No. 3. February 2015.
        num_replica = fault_tolerance+1
        num_leader = fault_tolerance+1
        num_acceptor = 2*fault_tolerance+1
    elif num_replica is None or num_leader is None or num_acceptor is None:
        print("Error: Either specifiy fault tolerance or number of replicas, leaders and acceptors")
    config_dict = create_config_dict(port, num_replica, num_leader, num_acceptor, config_file)
    
    procs = []
    #Start replica servers
    for replica in config_dict["replicas"]:
        procs.append(execute_replica(replica, config_file, path_to_folder))
    #Start acceptor servers
    for acceptor in config_dict["acceptors"]:
        procs.append(execute_acceptor(acceptor, path_to_folder))
    time.sleep(5)
    
    #Leader servers has to be started last as they will immedialy assume there exists some acceptors to scout
    for leader in config_dict["leaders"]:
        procs.append(execute_leader(leader,available_ports, config_file, path_to_folder))
    #Loop until shutdown
    while continous_run:
        pass

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
    import argparse

    #Setup argparser:
    parser = argparse.ArgumentParser(description="Startup script for Paxos system on UiT's cluster")
    parser.add_argument('-f', '--fault_tolerance', type=int, help="Automaticly choose number of replicas, acceptors and leaders based on needed fault tolerance")
    parser.add_argument('-c', '--config_file', type=str, default=CONFIG_FILE, help="Path to file where start script stores configuration needed for paxos servers")
    parser.add_argument('--folder', type=str, default="/home/vho023/3203/paxosmmc/code/backoff", help="Path to where on cluster the folder with executable is")
    parser.add_argument('-r', '--replicas', type=int, default=None,help="Number of replicas needed, will overwrite fault tolerance")
    parser.add_argument('-l', '--leaders', type=int, default=None,help="Number of leaders needed, will overwrite fault tolerance")
    parser.add_argument('-a', '--acceptors', type=int, default=None,help="Number of acceptors needed, will overwrite fault tolerance")
    parser.add_argument('-p', '--port', type=int, default=64209, help="Port hvere all paxos instance servers will be running")
    parser.add_argument('-pr', '--port_range', type=tuple, default=(64210,64310), help="Port range where leaders can spawn new scouts and commanders")
    parser.add_argument('--user',type=str, default='vho023', help="User which is running this paxos instance, used when shutting down processes")


    args = parser.parse_args()
    signal.signal(signal.SIGINT, kill_cluster)
    CONFIG_FILE = args.config_file

    
    run_on_cluster(fault_tolerance=args.fault_tolerance,
                     config_file=args.config_file,
                     port=args.port,
                     available_ports=args.port_range,
                     num_replica=args.replicas,
                     num_acceptor=args.acceptors,
                     num_leader=args.leaders,
                     path_to_folder=args.folder)