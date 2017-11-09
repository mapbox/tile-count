# Start from ubuntu
FROM ubuntu:17.04

# Update repos and install dependencies
RUN apt-get update \
  && apt-get -y upgrade \
  && apt-get -y install build-essential libsqlite3-dev zlib1g-dev libpng-dev

# Create a directory and copy in all files
RUN mkdir -p /tmp/tile-count-src
WORKDIR /tmp/tile-count-src
COPY . /tmp/tile-count-src

# Build tile-count
RUN make \
  && make install

# Run the tests
CMD make test
