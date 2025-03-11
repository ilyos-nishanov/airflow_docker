FROM apache/airflow:2.10.1

# Install system dependencies
USER root

# Install Oracle Instant Client dependencies (for cx_Oracle)
RUN apt-get update && \
    apt-get install -y \
    libaio1 \
    libcurl4-openssl-dev \
    libnsl-dev \
    gcc \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Download and install Oracle Instant Client
RUN wget https://download.oracle.com/otn_software/linux/instantclient/1925000/instantclient-basic-linux.x64-19.25.0.0.0dbru.zip -P /tmp \
    && unzip /tmp/instantclient-basic-linux.x64-19.25.0.0.0dbru.zip -d /opt \
    && ln -s /opt/instantclient_19_25 /opt/oracle \
    && echo /opt/oracle > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

# Install the SQL Server ODBC driver
RUN apt-get update && \
    apt-get install -y \
    unixodbc-dev \
    && wget https://packages.microsoft.com/keys/microsoft.asc -O /tmp/microsoft.asc \
    && apt-key add /tmp/microsoft.asc \
    && wget https://packages.microsoft.com/config/ubuntu/18.04/prod.list -O /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Install required Python packages
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

# Create necessary directories and set permissions
USER root
RUN mkdir -p /opt/airflow/logs && chmod -R 777 /opt/airflow/logs \
    && apt-get clean

# Set root password
RUN echo "root: g_host" | chpasswd

# Set airflow password
RUN echo "airflow:airflow" | chpasswd

# Clean up any unnecessary files to keep the image lean
RUN rm -rf /tmp/* /var/lib/apt/lists/* /etc/apt/sources.list.d/* /var/tmp/*



USER airflow