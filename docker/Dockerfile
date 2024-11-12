FROM ubuntu:latest

RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y git vim build-essential cmake wget sudo pkg-config

###install deps 

LABEL image.name="rtadp-c++"
LABEL image.description="rtadp-c++ software development enviroment"
LABEL org.opencontainers.image.authors="nicolo.parmiggiani@inaf.it ; luca.castaldini@inaf.it" 

USER root

# Set working directory
WORKDIR /root/deps
# Set the dependencies versions
# Versioni delle dipendenze
ARG BOOST_VERSION=1.86.0
ARG TCLAP_VERSION=1.2.5
ARG AVRO_VERSION=1.12.0
ARG LIBZMQ_VERSION=4.3.4
ARG CPPZMQ_VERSION=4.8.1

RUN echo -e "\033[0;34mDependencies selected versions\033[0m"
RUN echo -e "\033[0;32mBoost version: ${BOOST_VERSION}\033[0m"
RUN echo -e "\033[0;32mTclap version: ${TCLAP_VERSION}\033[0m"
RUN echo -e "\033[0;32mAvro: ${AVRO_VERSION}\033[0m"
RUN echo -e "\033[0;32mlibzmq: ${LIBZMQ_VERSION}\033[0m"
RUN echo -e "\033[0;32mcppzmq: ${CPPZMQ_VERSION}\033[0m"
# Install Boost
RUN echo -e "\033[0;34mInstalling Boost\033[0m"
RUN wget --no-check-certificate https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_$(echo "${BOOST_VERSION}" | sed 's/\./_/g').tar.bz2 -q && \
    tar --bzip2 -xf boost_$(echo "${BOOST_VERSION}" | sed 's/\./_/g').tar.bz2 && \
    cd boost_$(echo "${BOOST_VERSION}" | sed 's/\./_/g') && \
    ./bootstrap.sh && \
    ./b2 install
# Install tclap
RUN echo -e "\033[0;34mInstalling tclap\033[0m"
RUN wget --no-check-certificate https://sourceforge.net/projects/tclap/files/tclap-${TCLAP_VERSION}.tar.gz/download -O tclap.tar.gz -q && \
    tar -xzf tclap.tar.gz && \
    cd tclap-${TCLAP_VERSION} && \
    ./configure && \
    make -j8 && \
    make install
# Install Avro C++
RUN echo -e "\033[0;34mInstalling Avro C++\033[0m"
RUN wget --no-check-certificate http://archive.apache.org/dist/avro/avro-${AVRO_VERSION}/cpp/avro-cpp-${AVRO_VERSION}.tar.gz -q && \
    tar -xf avro-cpp-${AVRO_VERSION}.tar.gz && \
    cd avro-cpp-${AVRO_VERSION} && \
    ./build.sh install && \
    cp -r ./impl /usr/local/include/
# # Installazione di libzmq (ZeroMQ)
# RUN echo -e "\033[0;34mInstalling libzmq (ZeroMQ)\033[0m"
# RUN wget --no-check-certificate https://github.com/zeromq/libzmq/releases/download/v${LIBZMQ_VERSION}/zeromq-${LIBZMQ_VERSION}.tar.gz -q && \
#     tar -xzf zeromq-${LIBZMQ_VERSION}.tar.gz && \
#     cd zeromq-${LIBZMQ_VERSION} && \
#     mkdir build && cd build && \
#     cmake .. && \
#     make -j$(nproc) && \
#     make install && \
#     ldconfig

# # Installazione di cppzmq (header-only)
# RUN echo -e "\033[0;34mInstalling cppzmq (C++ binding for ZeroMQ)\033[0m"
# RUN wget --no-check-certificate https://github.com/zeromq/cppzmq/archive/refs/tags/v${CPPZMQ_VERSION}.tar.gz -O cppzmq-${CPPZMQ_VERSION}.tar.gz -q && \
#     tar -xzf cppzmq-${CPPZMQ_VERSION}.tar.gz && \
#     cd cppzmq-${CPPZMQ_VERSION} && \
#     mkdir build && cd build && \
#     cmake .. && \
#     make install


# # Crea l'utente 'gammaskydev'
# RUN useradd -ms /bin/bash gammaskydev && \
#     echo "gammaskydev ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# # Passa all'utente creato
# USER gammaskydev
