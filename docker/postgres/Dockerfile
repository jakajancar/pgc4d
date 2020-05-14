FROM postgres:10.13
ADD postgresql.conf \
    pg_hba.conf \
    postgres.pem \
    postgres-key.pem \
    /etc/postgresql/
RUN chown -R postgres:postgres /etc/postgresql && chmod 600 /etc/postgresql/postgres-key.pem
ADD init.sql /docker-entrypoint-initdb.d/init.sql
RUN mkdir /socket && chown postgres:postgres /socket
ENV POSTGRES_PASSWORD supersecret
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
