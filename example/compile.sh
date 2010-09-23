#!/bin/sh

java -cp obj/release/prod:third_party/java/obj:obj/release/test org.voltdb.compiler.VoltCompiler example/project.xml 1 1 127.0.0.1 out.jar