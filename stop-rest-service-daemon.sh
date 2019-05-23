#!/bin/sh

pid=jps | grep IKR | awk '{print $1}'
kill -9 $pid