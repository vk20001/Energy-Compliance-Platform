#!/bin/bash
# Run the original entrypoint first
/docker-entrypoint.sh "$@" &
PID=$!

# Wait for the properties file to be written
sleep 5

# Force admin security to PLAINTEXT regardless of what entrypoint wrote
echo "admin.security.protocol=PLAINTEXT" >> /kafka/config/connect-distributed.properties

# Now wait for the original process
wait $PID
