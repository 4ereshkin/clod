FROM postgis/postgis:16-3.4

RUN apt-get update && apt-get install -y \
  git \
  build-essential \
  postgresql-server-dev-16 \
  libxml2-dev \
  zlib1g-dev \
  autoconf \
  automake \
  libtool \
  && rm -rf /var/lib/apt/lists/*


RUN git clone https://github.com/pgpointcloud/pointcloud.git /tmp/pointcloud

WORKDIR /tmp/pointcloud
RUN ls -la
RUN autoreconf --install
RUN ./configure
RUN make with_llvm=no
RUN make install with_llvm=no

RUN rm -rf /tmp/pointcloud

WORKDIR /