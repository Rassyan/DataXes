# Start from a python image
FROM python:2.7.15-alpine3.7

# Default to UTF-8 file.encoding
ENV LANG C.UTF-8

# add a simple script that can auto-detect the appropriate JAVA_HOME value
# based on whether the JDK or only the JRE is installed
RUN { \
		echo '#!/bin/sh'; \
		echo 'set -e'; \
		echo; \
		echo 'dirname "$(dirname "$(readlink -f "$(which javac || which java)")")"'; \
	} > /usr/local/bin/docker-java-home \
	&& chmod +x /usr/local/bin/docker-java-home
ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk/jre
ENV PATH $PATH:/usr/lib/jvm/java-1.8-openjdk/jre/bin:/usr/lib/jvm/java-1.8-openjdk/bin

ENV JAVA_VERSION 8u181
ENV JAVA_ALPINE_VERSION 8.191.12-r0

RUN set -x \
	&& apk add --no-cache \
		openjdk8-jre="$JAVA_ALPINE_VERSION" \
		g++ \
	&& [ "$JAVA_HOME" = "$(docker-java-home)" ] \
	# jaydebeapi 会调用$JAVA_HOME/lib/amd64/libjvm.so，添加软连接
	&& ln -s $JAVA_HOME/lib/amd64/server/libjvm.so $JAVA_HOME/lib/amd64/libjvm.so \
	&& pip install jaydebeapi \
    && apk del g++ \
    && pip install pyyaml \
    && pip install prettytable \
    && pip install elasticsearch

ENV DATAX_HOME /opt/datax

ENV DATAXES_HOME /opt/dataxes

RUN mkdir -p ${DATAXES_HOME}

WORKDIR ${DATAXES_HOME}

ADD target/datax.tar.gz /opt/

CMD [ "/bin/sh" ]

VOLUME [ "${DATAX_HOME}" ]
