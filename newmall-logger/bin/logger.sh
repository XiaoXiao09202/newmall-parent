#!/bin/bash
JAVA_BIN=/home/appuser/app/jdk/bin/java
PROJECT=newmall-logger
APPNAME=newmall-logger-0.0.1-SNAPSHOT.jar
SERVER_PORT=8082

case $1 in
 "start")
   {
 
    for i in centos01 centos02 centos03
    do
     echo "========: $i==============="
    ssh $i  "$JAVA_BIN -Xms32m -Xmx64m  -jar /home/appuser/app/opt/$PROJECT/$APPNAME --server.port=$SERVER_PORT >/dev/null 2>&1  &"
    done
     echo "========NGINX==============="
    /usr/local/nginx/sbin/nginx
  };;
  "stop")
  { 
     echo "======== NGINX==============="
    /usr/local/nginx/sbin/nginx  -s stop
    for i in  centos01 centos02 centos03
    do
     echo "========: $i==============="
     ssh $i "ps -ef|grep $APPNAME |grep -v grep|awk '{print \$2}'|xargs kill" >/dev/null 2>&1
    done
 
  };;
   esac
 