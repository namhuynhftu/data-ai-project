FROM mysql:8.0

# Create secure directories with proper permissions
RUN mkdir -p /var/lib/mysql-files && \
    mkdir -p /var/run/mysqld && \
    mkdir -p /etc/mysql/certs && \
    chown -R mysql:mysql /var/lib/mysql-files /var/run/mysqld /etc/mysql/certs && \
    chmod 700 /var/lib/mysql-files /var/run/mysqld /etc/mysql/certs

# Copy configuration files
COPY ./conf.d/my.cnf /etc/mysql/conf.d/my.cnf
COPY ./docker-entrypoint-initdb.d/ /docker-entrypoint-initdb.d/

# Set correct permissions
RUN chown mysql:mysql /etc/mysql/conf.d/my.cnf && \
    chmod 600 /etc/mysql/conf.d/my.cnf && \
    chmod -R 755 /docker-entrypoint-initdb.d/