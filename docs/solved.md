## localstack:4566 port & podman

- see localstackport.md

## postgresql rc-service (SOLVED)

- psql -U postgres -d postgres" doesn't work
- stdout: elt/docs on  feat/alpha-vantage-etl [✘!+?] via 🐍 v3.13.7 (etl-batch) took 8s
  psql: error: connection to server on socket "/run/postgresql/.s.PGSQL.5432" failed: No such file or directory
  Is the server running locally and accepting connections on that socket?
- sudo rc-service postgresql start
  pg_ctl: control file appears to be corrupt

* ERROR: postgresql failed to start

### debugging

- cluster integrity "pg_controldata"
- file not corrupted "cluster state: shutdown"

### eureka

- sudo tail /var/lib/postgres/data/postmaster.log
  error FATAL: database files are incompatible with server, contradicting the message of pg_ctl.

## SOLVED (version conflict)

- /var/lib/postgres/data was created for version 17.0
- rc-service invokes the binary of version 18.0
- sudo mv *pg_dir* to pg_dir_v17 (bak)
- mkdir *pg_dir* given permissions to postgres user (sudo chown, chmod)
- initialize cluster: sudo -u postgres initdb -D /var/lib/postgres/data (using initdb util)
- start rc-service: sudo rc-service postgresql start
