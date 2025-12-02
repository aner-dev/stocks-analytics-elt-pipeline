## transformation module
- [ ] Test transformation with other symbols (AAPL, MSFT, TSLA)
- [ ] Implement batch orchestration for all symbols
- [ ] Add error handling for symbol-specific issues
# refactoring
## minio_client.py -> airflow s3Hook 
# logging 
- [ ] setup structlog in logging.py 
  - logging learning 
- solve compose: postgres service unhealthy 
- compose.yml: init-databases.sh not executing before airflow-init services initiate 
