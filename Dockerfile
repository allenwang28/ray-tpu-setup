# A very simple, reference Dockerfile.
FROM ubuntu:22.04

USER root


RUN apt-get upgrade -y && apt-get update -y && apt-get install -y \
    git \
    python3 \
    python3-pip \
    python3-dev \
    python-is-python3 \
    coreutils \
    rsync \
    openssh-client \
    curl \
    sudo \
    wget

RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

RUN useradd -m ray && echo "ray:docker" | chpasswd && adduser ray sudo
USER ray
WORKDIR /home/ray

RUN pip install cryptography
RUN pip install "ray[default]"

# Add ray to PATH
ENV PATH="/home/ray/.local/bin:${PATH}"

# Verify ray installation
RUN ray --version

# Install your ML framework of choice here...

# Create a startup script that waits...
RUN echo '#!/bin/bash\n\
echo "Container is running..."\n\
tail -f /dev/null' > /home/ray/startup.sh && \
    chmod +x /home/ray/startup.sh

ENTRYPOINT ["/home/ray/startup.sh"]
