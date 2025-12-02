# data engineering interview approaches

- **product sense**
- test me about questions on product sense and business needs
- figure out hot to do a SELF JOIN versus using a LAG or LEAD statement
  - self join preffer for hirers
- system design
  - database design, schemas,
- mencionar los 'puntos de dolor' en la interview
- resaltar los problemas con los que me encontre y las soluciones que tuve que usar
  - example:
  - "Usé [Framework] para la transformación, pero cuando el volumen de datos se incrementó, tuve que optimizar el proceso escalando el cluster y cambiando el método de join para reducir el shuffling (Componente 4 y 2)."

# transformation module

- [ ] Test transformation with other symbols (AAPL, MSFT, TSLA)

# refactoring

- [x] minio_client.py -> airflow s3Hook

# logging

- [x] setup structlog in logging.py

# compose

- [ ] fix compose.yml / init-databases.sh:
  - init-databases.sh not executing before airflow-init services initiate
  - sudo docker compose logs postgres
- [ ] create airflow connection (metadata db conn)

# testing

- [ ] unit test to hooks

# nvim-treesitter

- config queries for bash embedded in YAML files (syntax highlighting)
