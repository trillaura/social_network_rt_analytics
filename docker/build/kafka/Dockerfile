FROM openjdk:latest

ADD kafka_2.11-1.1.0.tgz /usr/local

ENV USR_LOCAL /usr/local
ENV KAFKA_HOME $USR_LOCAL/kafka

RUN ln -s ${USR_LOCAL}/kafka_2.11-1.1.0 $KAFKA_HOME

