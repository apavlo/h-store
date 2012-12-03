#!/usr/bin/python

"""A really dumb "state machine" generator."""


class Printer(object):
    def __init__(self):
        self.level= 0
        self.output = []

    def out(self, line):
        self.output.append(("    " * self.level) + line + "\n")

    def indent(self):
        self.level += 1

    def outdent(self):
        self.level -= 1
        assert self.level >= 0

    def __str__(self):
        return "".join(self.output)


def constantName(name):
    parts = name.split(" ")
    return "_".join((v.upper() for v in parts))

def methodName(name):
    parts = name.split(" ")
    return parts[0] + "".join((v.capitalize() for v in parts[1:]))


class StateMachine(object):
    def __init__(self, name, initial_state):
        self.name = name
        self.initial_state = initial_state
        self.states = set()
        self.states.add(initial_state)
        self.edges = {}

    def addTransition(self, state1, state2, label):
        self.states.add(state1)
        self.states.add(state2)

        if state1 not in self.edges:
            self.edges[state1] = {}
        assert state2 not in self.edges[state1]
        self.edges[state1][state2] = label

    def getLabels(self):
        labels = {}
        for source, destinations in self.edges.iteritems():
            for destination, label in destinations.iteritems():
                if label not in labels:
                    labels[label] = []
                labels[label].append((source, destination))
        return labels

    def toCpp(self):
        p = Printer()
        p.out("class %s {" % (self.name))
        p.out("public:")
        # constructor and reset
        p.out("    %s() { reset(); }" % (self.name))
        p.out("    void reset() { state_ = %s; }" % (constantName(self.initial_state)))
        p.out("")

        # set* methods
        labels = self.getLabels()
        for label, src_dsts in labels.iteritems():
            p.indent()
            p.out("void %s() {" % (methodName("set " + label)))
            p.indent()

            statement = "if"
            for src, dst in src_dsts:
                p.out("%s (state_ == %s) {" % (statement, constantName(src)))
                p.indent()
                p.out("state_ = %s;" % (constantName(dst)))
                p.outdent()
                statement = "} else if"
            p.out("} else {")
            p.out("    assert(false);")
            p.out("}")
            p.outdent()
            p.out("}")

            p.outdent()
            p.out("")

        # is* methods
        p.indent()
        for state_name in self.states:
            p.out("bool %s() const { return state_ == %s; }" % (
                    methodName("is " + state_name), constantName(state_name)))
        p.outdent()
        p.out("")

        p.out("private:")
        p.indent()
        p.out("enum State {")
        p.indent()

        for state_name in self.states:
            p.out("%s," % (constantName(state_name)))

        #~ p.out(
        p.outdent()
        p.out("} state_;")
        p.outdent()
        p.out("};")
        return str(p)

    def toDot(self):
        out = ""
        out += "digraph %s {\n" % (self.name)
        out += "    node [shape=rectangle,fontname=Helvetica];\n"
        out += "    edge [fontname=Helvetica];\n"

        for source, destinations in self.edges.iteritems():
            for destination, label in destinations.iteritems():
                out += '    "%s" -> "%s" [label="%s"];\n' % (source, destination, label)

        out += "}\n"
        return out


if __name__ == "__main__":
    participant = StateMachine("ParticipateState", "unknown")
    participant.addTransition("unknown", "involved", "write")
    participant.addTransition("unknown", "finished", "done")
    participant.addTransition("involved", "preparing", "done")
    participant.addTransition("preparing", "prepared", "ack")
    participant.addTransition("preparing", "finished", "single partition ack")
    participant.addTransition("prepared", "finished", "commit")
    #~ print participant.toDot()

    transaction = StateMachine("TransactionStateMachine", "unfinished")
    transaction.addTransition("unfinished", "unfinished fragment", "unfinished fragment")
    transaction.addTransition("unfinished fragment", "unfinished", "executed")
    transaction.addTransition("unfinished", "finished fragment", "finished fragment")

    transaction.addTransition("finished fragment", "finished executed", "executed")
    transaction.addTransition("finished fragment", "finished replicated", "replicated")
    transaction.addTransition("finished replicated", "prepared", "executed")
    transaction.addTransition("finished executed", "prepared", "replicated")

    transaction.addTransition("prepared", "decided executed", "decided")
    transaction.addTransition("prepared", "decided replicated", "replicated")
    transaction.addTransition("prepared", "done", "single partition done")
    transaction.addTransition("decided executed", "done", "replicated")
    transaction.addTransition("decided replicated", "done", "executed")

    print transaction.toCpp()
    #~ print transaction.toDot()
