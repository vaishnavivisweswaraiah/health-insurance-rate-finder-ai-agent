#!/bin/bash

# Use Java 11 only for this project
export JAVA_HOME="$PWD/sparkJava/jdk-11.0.26+4/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

echo "Using JAVA from: $JAVA_HOME"
$JAVA_HOME/bin/java -version


code .