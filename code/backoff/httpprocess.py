from message import *
import multiprocessing
from threading import Thread
import pickle
import requests
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class Handler(BaseHTTPRequestHandler):
    """
    Handler for a Process to get new messages
    """
    def do_POST(self):
        #Get size from path
        size = int(self.path.split("/")[-1])
        
        #Read pickled message
        msg=self.rfile.read(size)
        try:
            #Depickle message
            msg = pickle.loads(msg)
        except pickle.PickleError:
            self.send_response(400)
            return
    
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        #Deliver message to self
        self.server.deliver(msg)


class Process(ThreadingHTTPServer):
    """
    A process is a http server with a queue of incoming messages, and an
    "environment" that keeps track of all processes and queues.
    """
    def __init__(self,server_address, handler_class, id):
        super().__init__(server_address, handler_class)
        self.thread = Thread(target=self.run)
        self.inbox = multiprocessing.Manager().Queue()
        self.id = id
        self.thread.start()

    def run(self):
        try:
            self.body()
            self.env.removeProc(self.id)
        except EOFError:
            print("Exiting..")

    def getNextMessage(self):
        return self.inbox.get()

    
    def sendMessage(self, dst, msg):
        """
        dst: Destination of message, as endpoint
        msg: Message object
        """
        pickled_msg = pickle.dumps(msg)
        requests.post("http://"+dst+f"/{len(pickled_msg)}", data=pickled_msg)
        # self.env.sendMessage(dst, msg)


    def deliver(self, msg):
        """
        msg: Message object
        """
        print(msg)
        self.inbox.put(msg)

if __name__ == "__main__":
    HOST, PORT = "127.0.0.1", 5555
    with Process((HOST, PORT), Handler, 2, 1) as server:
        server.serve_forever()
