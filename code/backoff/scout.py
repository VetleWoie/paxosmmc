# from process import Process
from httpprocess import Process
from message import P1aMessage, P1bMessage, PreemptedMessage, AdoptedMessage

class Scout(Process):
    """
    The scout runs what is known as phase 1 of the Synod protocol.
    Every scout is created for a specific ballot number.
    """
    def __init__(self, server_address, handler_class, id, leader, acceptors, ballot_number, port_handler):
        self.leader = leader
        self.acceptors = acceptors
        self.ballot_number = ballot_number
        self.port_handler = port_handler
        Process.__init__(self, server_address, handler_class, id)

    def shutdown(self) -> None:
        super().shutdown()
        self.port_handler.put(self.server_address[1])

    def body(self):
        """
        A scout sends a p1a message to all acceptors, and waits for
        p1b responses. In each such response the ballot number in the
        message will be greater than the ballot number of the
        scout. There are two cases:

        - When a scout receives a p1b message it records all the
        values that are accepted by the acceptor that sent it. If the
        scout receives such p1b messages from all acceptors in a
        majority of acceptors, then the scout learns that its ballot
        number is adopted. In this case, the commander notifies its
        leader and exits.

        - If a scout receives a p1b message with a different
        ballot number from some acceptor, then it learns that a higher
        ballot is active. This means that the scout's
        ballot number may no longer be able to make progress. In this
        case, the scout notifies its leader about the existence of
        the higher ballot number, and exits.
        """
        waitfor = set()
        for a in self.acceptors:
            self.sendMessage(a, P1aMessage(self.id, self.ballot_number))
            waitfor.add(a)

        pvalues = set()
        while True:
            msg = self.getNextMessage()
            if isinstance(msg, P1bMessage):
                if self.ballot_number == msg.ballot_number and msg.src in waitfor:
                    pvalues.update(msg.accepted)
                    waitfor.remove(msg.src)
                    if len(waitfor) < float(len(self.acceptors))/2:
                        self.sendMessage(self.leader,
                                         AdoptedMessage(self.id,
                                                        self.ballot_number,
                                                        pvalues))
                        return self.shutdown()
                else:
                    self.sendMessage(self.leader,
                                     PreemptedMessage(self.id,
                                                      msg.ballot_number))
                    return self.shutdown()
            else:
                print("Scout: unexpected msg")

if __name__ == "__main__":
    import multiprocessing
    from httpprocess import Handler
    import sys
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} [adress] [configfile]")
        exit(0)

    adress = sys.argv[1].split(':')
    port_handler = multiprocessing.Manager().Queue()
    scout = Scout((adress[0], int(adress[1])), Handler, sys.argv[1], None, None, None,port_handler)
    port = port_handler.get()
    print(port)
    