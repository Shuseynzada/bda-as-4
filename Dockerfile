# Use the official Jupyter PySpark notebook image as a base
FROM jupyter/pyspark-notebook:latest

# Switch to root user to perform administrative tasks
USER root

# Install additional dependencies
RUN apt-get update && \
    apt-get install -y wget gnupg2 curl && \
    rm -rf /var/lib/apt/lists/*

# Install OpenJDK from Adoptium
RUN mkdir -p /usr/share/man/man1 && \
    curl -L -o /tmp/openjdk.tar.gz https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.15+10/OpenJDK11U-jdk_x64_linux_hotspot_11.0.15_10.tar.gz && \
    tar -C /opt -xzf /tmp/openjdk.tar.gz && \
    rm /tmp/openjdk.tar.gz && \
    ln -s /opt/jdk-11.0.15+10 /opt/jdk-11

# Set JAVA_HOME
ENV JAVA_HOME=/opt/jdk-11
ENV PATH=$JAVA_HOME/bin:$PATH

# Create a directory for the application
RUN mkdir /app

# Switch back to the original non-root user
USER $NB_UID

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt /app/requirements.txt

# Install Python dependencies, including pyspark
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the current directory contents into the container at /app
COPY . /app

# Set Spark configuration to allocate more memory
ENV SPARK_DRIVER_MEMORY 4g
ENV SPARK_EXECUTOR_MEMORY 4g

# Command to run the PySpark script
CMD ["python", "/app/main.py"]
