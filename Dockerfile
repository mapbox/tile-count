# Start from ubuntu
FROM ubuntu:17.04

# Update repos and install dependencies
RUN apt-get update \
  && apt-get -y upgrade \
  && apt-get -y install build-essential wget libsqlite3-dev zlib1g-dev libpng-dev

# Create a directory and copy in all files
RUN mkdir -p /tmp/tile-count-src
WORKDIR /tmp/tile-count-src
COPY . /tmp/tile-count-src

# Build tile-count
RUN make \
  && make install

# Install tippecanoe since the tests depend on it
ENV TIPPECANOE_VERSION="1.26.3"

RUN wget https://github.com/mapbox/tippecanoe/archive/${TIPPECANOE_VERSION}.tar.gz  && \
    tar -xvf ${TIPPECANOE_VERSION}.tar.gz && \
    cd tippecanoe-${TIPPECANOE_VERSION} && \
    make && \
    make install


# Run the tests
CMD make test
