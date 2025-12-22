time.sleep(65) in 'extract_stocks.py' was useless until add '@task(max_active_tis_per_dag=1)'
that argument act as a semaforo, and give sense to that 'time.sleep(65)'
obligates airflow to do 1 task at time, even it had 10 tasks mapped
