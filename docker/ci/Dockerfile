FROM debian:buster

# Install debian packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    unzip

# Install deno
RUN cd /usr/local/bin && \
    curl -sSL -o deno.zip https://github.com/denoland/deno/releases/download/v1.0.2/deno-x86_64-unknown-linux-gnu.zip && \
    unzip deno.zip && \
    chmod +x deno && \
    rm deno.zip
