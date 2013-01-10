#!/bin/sh

java -Djava.library.path=obj/release/nativelibs -cp obj/release/prod:third_party/java/obj:obj/release/test edu.mit.VoltProcedureInvoker out.jar

# Run with the eclipse build:
#~ java -cp obj/eclipse:third_party/java/obj edu.mit.VoltProcedureInvoker out.jar
