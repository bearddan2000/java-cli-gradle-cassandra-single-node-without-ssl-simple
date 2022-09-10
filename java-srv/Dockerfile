FROM gradle:jdk11

USER root

VOLUME "/root/log"

WORKDIR /app

ADD --chown=gradle:gradle /bin/ .

RUN chmod -R +x *

# Add docker-compose-wait tool -------------------
ENV WAIT_VERSION 2.7.2

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/$WAIT_VERSION/wait /wait

RUN chmod +x /wait

CMD ["gradle", "run"]
