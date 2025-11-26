#!/bin/bash
# Submit PyFlink bucketed rolling sum job to Flink cluster

PYTHON_FILE="rolling_30day_bucketed.py"

echo "=== Submitting PyFlink Bucketed Rolling Sum Job ==="

# Copy Python file to Flink containers
echo "Copying Python file to containers..."
docker cp "$PYTHON_FILE" flink_jobmanager:/tmp/
docker cp "$PYTHON_FILE" flink_taskmanager:/tmp/

# Submit to Flink cluster using flink run with Python
echo "Submitting to Flink cluster..."
docker exec flink_jobmanager bash -c "cd /opt/flink && ./bin/flink run --python /tmp/$PYTHON_FILE --detached"

echo "=== Job submitted ==="
echo "Check status: http://localhost:8082"
