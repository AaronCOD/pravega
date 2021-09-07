#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Note: This file contains source code copied from the Apache BookKeeper
#       project (http://bookkeeper.apache.org). Specifically, it contains
#       source code copied from file `Dockerfile`, revisions `a14c8bf` and
#       `a00f60d`:
# - github.com/apache/bookkeeper/blob/branch-4.7/docker/Dockerfile
# - github.com/apache/bookkeeper/blob/branch-4.9/docker/Dockerfile
#

FROM apache/bookkeeper:4.14.1

ARG BK_VERSION=4.14.1
ARG DISTRO_NAME=bookkeeper-all-${BK_VERSION}-bin

RUN set -x \
    && yum install -y iproute wget \
    && cd /opt \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz" \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz.asc" \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz.sha512" \
    && sha512sum -c ${DISTRO_NAME}.tar.gz.sha512 \
    && wget https://dist.apache.org/repos/dist/release/bookkeeper/KEYS \
    && gpg --import KEYS \
    && gpg --batch --verify "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz" \
    && tar -xzf "$DISTRO_NAME.tar.gz" \
    && cp -r bookkeeper-all-${BK_VERSION}/* /opt/bookkeeper/ \
    && rm -rf "bookkeeper-all-${BK_VERSION}" "$DISTRO_NAME.tar.gz" "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz.sha512" \
    && yum clean all

WORKDIR /opt/bookkeeper

COPY entrypoint.sh /opt/bookkeeper/scripts/pravega_entrypoint.sh

# For backwards compatibility with older operator versions
COPY entrypoint.sh /opt/bookkeeper/entrypoint.sh
COPY log4j.properties /opt/bookkeeper/conf/log4j.properties
COPY bk-sp.sh /
RUN chmod +x /bk-sp.sh

RUN chmod +x -R /opt/bookkeeper/scripts/
RUN chgrp -R 0 /opt/bookkeeper && \
    chmod -R g=u /opt/bookkeeper
ENV BOOKIE_LOG_CONF=/opt/bookkeeper/conf/log4j.properties
ENV BOOKIE_LOG_DIR=/opt/bookkeeper/logs
ENV BOOKIE_ROOT_LOGGER=INFO,ROLLINGFILE
RUN sed -i 's/-Xlog:gc=info:file=\${LOG_DIR}\/\${GC_LOG_FILENAME}::filecount=5,filesize=64m//g' /opt/bookkeeper/bin/common.sh

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/scripts/pravega_entrypoint.sh" ]
CMD ["bookie"]

HEALTHCHECK --interval=10s --timeout=60s CMD /bin/bash /opt/bookkeeper/scripts/healthcheck.sh
