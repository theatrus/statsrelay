FROM lyft/opsbase:6041c9b59c17421b60e026f06b866672dceae752
COPY . /code/statsrelay-private
RUN apt-get update -y && \
    apt-get install -y software-properties-common python-software-properties && \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
    apt-get update -y && \
    apt-get install -y g++-4.9 cmake libssl-dev clang-format-3.6 autoconf libtool pkg-config check libcurl4-openssl-dev libev-dev libjansson-dev libpcre3-dev libyaml-dev pkg-config && \
    cd /code/statsrelay-private && \
    rm -f CMakeCache.txt && \
    cmake . && make && make test && make integ && cp statsrelay /usr/bin/statsrelay && \
    apt-get purge -y software-properties-common python-software-properties g++-4.9 cmake libssl-dev clang-format-3.6 autoconf libtool pkg-config check libcurl4-openssl-dev libev-dev libjansson-dev libpcre3-dev libyaml-dev pkg-config && \
    apt-get autoremove -y