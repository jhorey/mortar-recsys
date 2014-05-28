FROM ferry/hadoop-client
NAME ferry/mortar-recsys

# Install everything
WORKDIR /home/ferry
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get --yes install python-pip git
RUN git clone https://github.com/jhorey/mortar-recsys.git

# Make Ferry the owner
RUN chown -R ferry:ferry /home/ferry/mortar-recsys

# Add the test script
ADD ./testmortar.sh /service/runscripts/test/
RUN chmod a+x /service/runscripts/test/testmortar.sh

# Modify path
RUN echo export PATH=$$PATH:$$HADOOP_HOME/bin:$$PIG_HOME/bin >> /etc/profile

# Clean everything
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
