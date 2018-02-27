#!/bin/sh
cd lib
mvn clean install
cd ../binary-file-output
mvn clean install
cd ../pst-input
mvn clean install
