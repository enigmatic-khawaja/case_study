#!/bin/bash
set -e

# Wait for SQL Server to be ready
echo "Waiting for SQL Server to become available..."
for i in {1..60}; do
  if python -c "
import pymssql
try:
  conn = pymssql.connect(
    server='sqlserver',
    user='sa',
    password='YourStrong@Passw0rd',
    database='master',
    timeout=10
  )
  conn.close()
  print('SQL Server ready!')
  exit(0)
except Exception as e:
  print(f'Attempt {i}/60: SQL Server not ready yet ({str(e)}')
  exit(1)
"; then
    break
  fi
  sleep 10
done

# Start Jupyter
exec jupyter lab \
  --ip=0.0.0.0 \
  --port=${JUPYTER_PORT:-8888} \
  --no-browser \
  --allow-root \
  --NotebookApp.token=${JUPYTER_TOKEN:-etlpipeline} \
  --notebook-dir=/app/notebooks