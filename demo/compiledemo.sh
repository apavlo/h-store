#!/bin/bash
git pull
ant clean-java build-java
ant hstore-prepare -Dproject=voterdemosstorecorrect -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterdemohstorecorrect -Dhosts="localhost:0:0"