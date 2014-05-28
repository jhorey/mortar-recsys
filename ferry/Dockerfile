FROM ferry/hadoop-client
NAME ferry/mortar-recsys

# Install everything
WORKDIR /home/ferry
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get --yes install python-pip git
RUN git clone https://github.com/jhorey/mortar-recsys.git

# Make Ferry the owner
RUN chown -R ferry:ferry /home/ferry/mortar-recsys

# Clean everything
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
