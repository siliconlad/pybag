FROM ros:humble

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

# Setup environment
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV ROS_DISTRO humble

# Install basic dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    curl \
    wget \
    lsb-release \
    vim \
    tree \
    ros-humble-rosbag2-storage-mcap \
    && rm -rf /var/lib/apt/lists/*

# Install MCAP CLI
RUN curl -L "https://github.com/foxglove/mcap/releases/download/releases%2Fcli%2Fv0.0.42/mcap-linux-$(dpkg --print-architecture)" -o /usr/local/bin/mcap && chmod +x /usr/local/bin/mcap

# Install uv package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:${PATH}"

# Source ROS environment in bashrc
# Uncommenting causes problems with uv environments (pyyaml + lark)
# RUN echo 'source /opt/ros/'$ROS_DISTRO'/setup.bash' >> /root/.bashrc

# Git Config
RUN git config --global push.autoSetupRemote true
RUN git config --global core.editor "vim"
RUN git config --global --add safe.directory /pybag
RUN git config --global push.default current


# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=

WORKDIR /pybag
