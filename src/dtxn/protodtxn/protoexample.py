#!/usr/bin/python

"""Simple script to start all the protodtxn test processes."""

import os
import signal
import subprocess
import time


# Needed for older versions of Python, where KeyboardInterrupt is not an Exception subclass
__pychecker__ = "no-badexcept"


JAVADTXN = os.environ["HOME"] + "/javadtxn"
CLASSPATH = JAVADTXN + "/build/java"
JAVA_COMMAND = ("java", "-server", "-ea", "-cp", CLASSPATH)

SERVER = "edu.mit.ExampleServer"
COORDINATOR = "edu.mit.ExampleCoordinator"
SERVER_START_PORT = 22345
SERVERS = 2

ENGINE_START_PORT = 12345
PROTOENGINE = "build/protodtxn/protodtxnengine"
PROTOCOORD = "build/protodtxn/protodtxncoordinator"

COORDINATOR_PORT = 12347
CONFIG = "hstore.conf"


class ProcessList(object):
    def __init__(self):
        self.servers = []
        self.processGroup = 0

        # Register sigterm handler to clean up the child processes correctly
        # TODO: Make this play nice with other handlers?
        def sigTermHandler(signum, _):
            assert signum == signal.SIGTERM
            self.term()

            # Redeliver the signal to kill ourselves
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            os.kill(0, signal.SIGTERM)
        signal.signal(signal.SIGTERM, sigTermHandler)

    def int(self):
        self.servers.reverse()
        for server in self.servers:
            if server.poll() is None:
                os.kill(server.pid, signal.SIGINT)
            data = server.stdout.read()
            server.wait()
            if data != "":
                print "STDOUT:"
                print data
        self.servers = []

    def term(self):
        if self.processGroup == 0:
            for i, server in enumerate(self.servers):
                os.kill(server.pid, signal.SIGTERM)
        else:
            os.killpg(self.processGroup, signal.SIGTERM)
        self.servers = []

    def __del__(self):
        self.int()

    def setChildProcessGroup(self):
        os.setpgid(0, self.processGroup)

    def start(self, command):
        print command
        # Run os.setsid in the child to "detach": SIGINT won't kill children
        server = subprocess.Popen(command, stdout=subprocess.PIPE, preexec_fn=self.setChildProcessGroup)
        if self.processGroup == 0:
            self.processGroup = server.pid
        self.servers.append(server)

        #~ self.commands.append(command)
        time.sleep(1)
        assert server.poll() is None
        return server


if __name__ == "__main__":
    processes = ProcessList()

    # Start Java servers
    for i in xrange(SERVERS):
        command = JAVA_COMMAND + (SERVER, str(SERVER_START_PORT + i))
        processes.start(command)

    # Start protodtxn
    for i in xrange(SERVERS):
        command = (PROTOENGINE, "localhost:" + str(SERVER_START_PORT + i), CONFIG, str(i), "0")
        processes.start(command)

    command = (PROTOCOORD, str(ENGINE_START_PORT + SERVERS), CONFIG)
    processes.start(command)

    command = JAVA_COMMAND + (COORDINATOR, "localhost", str(COORDINATOR_PORT), str(SERVER_START_PORT + SERVERS))
    coordinator = processes.start(command)
    try:
        coordinator.wait()
    except KeyboardInterrupt:
        processes.int()
