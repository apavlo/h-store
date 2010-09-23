all: libBench.so
	java -Djava.library.path=. -server -XX:+AggressiveOpts -XX:CompileThreshold=1 Bench

libBench.so: Bench.cc Bench.h
	g++ -shared -fPIC \
		-I/usr/lib/jvm/java-6-openjdk/include/ \
		-I/usr/lib/jvm/java-6-openjdk/include/linux \
		-o libBench.so Bench.cc

Bench.h: Bench.class
	javah -jni Bench

Bench.class: Bench.java
	javac Bench.java

clean:
	rm -f libBench.so Bench.h Bench.class

.PHONY: clean
