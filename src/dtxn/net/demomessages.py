#!/usr/bin/python

import stupidcompiler

demo = stupidcompiler.MessageDefinition("Demo", "A simple test message.")
demo.addField(stupidcompiler.INT32, "integer", -42, "An integer.")
demo.addField(stupidcompiler.BOOL, "boolean", False, "A boolean.")
demo.addField(stupidcompiler.STRING, "str", None, "A string.")

messages = [demo]

if __name__ == "__main__":
    stupidcompiler.main(messages, "net")
