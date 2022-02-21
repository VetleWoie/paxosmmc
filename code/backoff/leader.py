from utils import *
# from process import Process
import threading
import multiprocessing
from httpprocess import Process, Handler
from commander import Commander
from scout import Scout
from message import ProposeMessage, AdoptedMessage, PreemptedMessage
from time import sleep

class Leader(Process):
    """
    Leader receives requests from replicas, serializes requests and
    responds to replicas. Leader maintains four state variables:
    - ballot_number: a monotonically increasing ballot number
    - active: a boolean flag, initially false
    - proposals: a map of slot numbers to proposed commands in the form
    of a set of (slot number, command) pairs, initially empty. At any
    time, there is at most one entry per slot number in the set.
    - timeout: time in seconds the leader waits between operations
    """
    def __init__(self, server_address, handler_class, id, config, available_ports):
        self.id = id
        self.ballot_number = BallotNumber(0, self.id)
        self.active = False
        self.proposals = {}
        self.timeout = 1.0
        self.config = config
        self.threads = []
        #Keep track of what ports can be opened for scouts and commanders
        self.available_ports = multiprocessing.Manager().Queue()
        for port in range(available_ports[0], available_ports[1]):
            self.available_ports.put(port)
        Process.__init__(self, server_address, handler_class, id)
    
    def new_scout(self):
        """
        Find available port and spawn a new scout as a new thread.
        """
        print("Spawning new scout")
        port = self.available_ports.get()
        thread = threading.Thread(target=Scout,
                args=[(self.server_address[0], port), 
                        Handler, 
                        self.server_address[0]+":"+str(port), 
                        self.id,
                        self.config.acceptors, 
                        self.ballot_number, 
                        self.available_ports])
        self.threads.append(thread)
        thread.start()

    def new_commander(self, slot_number, command):
        """
        Find available port and spawn a new scout as a new thread.
        """
        print("Spawning new commander")
        port = self.available_ports.get()
        thread = threading.Thread(target=Commander,
                args=[(self.server_address[0], port), 
                        Handler, 
                        self.server_address[0]+":"+str(port), 
                        self.id,
                        self.config.acceptors,
                        self.config.replicas,
                        self.ballot_number,
                        slot_number,
                        command,
                        self.available_ports
                        ])
        self.threads.append(thread)
        thread.start()

    def body(self):
        """
        The leader starts by spawning a scout for its initial ballot
        number, and then enters into a loop awaiting messages. There
        are three types of messages that cause transitions:

        - Propose: A replica proposes given command for given slot number

        - Adopted: Sent by a scout, this message signifies that the
        current ballot number has been adopted by a majority of
        acceptors. (If an adopted message arrives for an old ballot
        number, it is ignored.) The set pvalues contains all pvalues
        accepted by these acceptors prior to the adopted ballot
        number.

        - Preempted: Sent by either a scout or a commander, it means
        that some acceptor has adopted the ballot number that is
        included in the message. If this ballot number is higher than
        the current ballot number of the leader, it may no longer be
        possible to use the current ballot number to choose a command.
        """
        print("Here I am: ", self.id)
        #Create new scout
        self.new_scout()
        while True:
            msg = self.getNextMessage()
            if isinstance(msg, ProposeMessage):
                if msg.slot_number not in self.proposals:
                    self.proposals[msg.slot_number] = msg.command
                    if self.active:
                        self.new_commander(msg.slot_number, msg.command)
                        # Commander(self.env,"commander:%s:%s:%s" % (str(self.id),
                        #                                            str(self.ballot_number),
                        #                                            str(msg.slot_number)),
                        #           self.id, self.config.acceptors, self.config.replicas,
                        #           self.ballot_number, msg.slot_number, msg.command)
            elif isinstance(msg, AdoptedMessage):
                # Decrease timeout since the leader does not seem to
                # be competing with another leader.
                if self.timeout > TIMEOUTSUBTRACT:
                    self.timeout = self.timeout - TIMEOUTSUBTRACT
                    print(self.id, "Timeout decreased: ", self.timeout)
                if self.ballot_number == msg.ballot_number:
                    pmax = {}
                    # For every slot number add the proposal with
                    # the highest ballot number to proposals
                    for pv in msg.accepted:
                        if pv.slot_number not in pmax or \
                              pmax[pv.slot_number] < pv.ballot_number:
                            pmax[pv.slot_number] = pv.ballot_number
                            self.proposals[pv.slot_number] = pv.command
                    # Start a commander (i.e. run Phase 2) for every
                    # proposal (from the beginning)
                    for sn in self.proposals:
                        self.new_commander(sn, self.proposals.get(sn))
                        # Commander(self.env,
                        #           "commander:%s:%s:%s" % (str(self.id),
                        #                                   str(self.ballot_number),
                        #                                   str(sn)),
                        #           self.id, self.config.acceptors, self.config.replicas,
                        #           self.ballot_number, sn, self.proposals.get(sn))
                    self.active = True
            elif isinstance(msg, PreemptedMessage):
                # The leader is competing with another leader
                if msg.ballot_number.leader_id > self.id:
                    # Increase timeout because the other leader has priority
                    self.timeout = self.timeout * TIMEOUTMULTIPLY
                    print(self.id, "Timeout increased: ", self.timeout)
                if msg.ballot_number > self.ballot_number:
                    self.active = False
                    self.ballot_number = BallotNumber(msg.ballot_number.round+1,
                                                      self.id)
                    #Spawn new scout
                    self.new_scout()
            else:
                print("Leader: unknown msg type")
            sleep(self.timeout)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} [adress] [configfile]")
        exit(0)
    adress = sys.argv[1].split(':')
    config = Config.from_jsonfile(sys.argv[2])
    Leader((adress[0], int(adress[1])), Handler, sys.argv[1],config,(6000,6010))
    #Start replica server
    # Replica((adress[0], int(adress[1])), Handler, sys.argv[1], config)

    # with Replica((adress[0], int(adress[1])), Handler, sys.argv[1], config) as r: 
        # print("Starting replica server")
        # r.serve_forever()
