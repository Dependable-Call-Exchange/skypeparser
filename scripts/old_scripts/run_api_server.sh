#!/bin/bash
# Run the Skype Parser API server and Celery worker

# Set default values
API_HOST="0.0.0.0"
API_PORT="5000"
WORKER_CONCURRENCY="2"
REDIS_URL="redis://localhost:6379/0"
LOG_LEVEL="INFO"
ASYNC_THRESHOLD="52428800"  # 50MB in bytes

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --api-host)
      API_HOST="$2"
      shift 2
      ;;
    --api-port)
      API_PORT="$2"
      shift 2
      ;;
    --worker-concurrency)
      WORKER_CONCURRENCY="$2"
      shift 2
      ;;
    --redis-url)
      REDIS_URL="$2"
      shift 2
      ;;
    --log-level)
      LOG_LEVEL="$2"
      shift 2
      ;;
    --async-threshold)
      ASYNC_THRESHOLD="$2"
      shift 2
      ;;
    --api-key)
      API_KEY="$2"
      shift 2
      ;;
    --db-name)
      DB_NAME="$2"
      shift 2
      ;;
    --db-user)
      DB_USER="$2"
      shift 2
      ;;
    --db-password)
      DB_PASSWORD="$2"
      shift 2
      ;;
    --db-host)
      DB_HOST="$2"
      shift 2
      ;;
    --db-port)
      DB_PORT="$2"
      shift 2
      ;;
    --upload-folder)
      UPLOAD_FOLDER="$2"
      shift 2
      ;;
    --output-folder)
      OUTPUT_FOLDER="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --api-host HOST            Host to bind the API server to (default: 0.0.0.0)"
      echo "  --api-port PORT            Port to bind the API server to (default: 5000)"
      echo "  --worker-concurrency N     Number of worker processes (default: 2)"
      echo "  --redis-url URL            Redis URL for Celery (default: redis://localhost:6379/0)"
      echo "  --log-level LEVEL          Logging level (default: INFO)"
      echo "  --async-threshold BYTES    File size threshold for asynchronous processing in bytes (default: 50MB)"
      echo "  --api-key KEY              API key for authentication"
      echo "  --db-name NAME             Database name"
      echo "  --db-user USER             Database user"
      echo "  --db-password PASSWORD     Database password"
      echo "  --db-host HOST             Database host (default: localhost)"
      echo "  --db-port PORT             Database port (default: 5432)"
      echo "  --upload-folder FOLDER     Directory for temporary file uploads"
      echo "  --output-folder FOLDER     Directory for ETL pipeline output"
      echo "  --help                     Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Set environment variables
export CELERY_BROKER_URL="$REDIS_URL"
export CELERY_RESULT_BACKEND="$REDIS_URL"

if [ ! -z "$API_KEY" ]; then
  export API_KEY="$API_KEY"
fi

if [ ! -z "$DB_NAME" ]; then
  export DB_NAME="$DB_NAME"
fi

if [ ! -z "$DB_USER" ]; then
  export DB_USER="$DB_USER"
fi

if [ ! -z "$DB_PASSWORD" ]; then
  export DB_PASSWORD="$DB_PASSWORD"
fi

if [ ! -z "$DB_HOST" ]; then
  export DB_HOST="$DB_HOST"
fi

if [ ! -z "$DB_PORT" ]; then
  export DB_PORT="$DB_PORT"
fi

# Build the common arguments
COMMON_ARGS=""

if [ ! -z "$UPLOAD_FOLDER" ]; then
  COMMON_ARGS="$COMMON_ARGS --upload-folder $UPLOAD_FOLDER"
fi

if [ ! -z "$OUTPUT_FOLDER" ]; then
  COMMON_ARGS="$COMMON_ARGS --output-folder $OUTPUT_FOLDER"
fi

if [ ! -z "$API_KEY" ]; then
  COMMON_ARGS="$COMMON_ARGS --api-key $API_KEY"
fi

if [ ! -z "$DB_NAME" ]; then
  COMMON_ARGS="$COMMON_ARGS --db-name $DB_NAME"
fi

if [ ! -z "$DB_USER" ]; then
  COMMON_ARGS="$COMMON_ARGS --db-user $DB_USER"
fi

if [ ! -z "$DB_PASSWORD" ]; then
  COMMON_ARGS="$COMMON_ARGS --db-password $DB_PASSWORD"
fi

if [ ! -z "$DB_HOST" ]; then
  COMMON_ARGS="$COMMON_ARGS --db-host $DB_HOST"
fi

if [ ! -z "$DB_PORT" ]; then
  COMMON_ARGS="$COMMON_ARGS --db-port $DB_PORT"
fi

COMMON_ARGS="$COMMON_ARGS --log-level $LOG_LEVEL --redis-url $REDIS_URL --async-threshold $ASYNC_THRESHOLD"

# Check if Redis is running
echo "Checking if Redis is running..."
if ! redis-cli ping > /dev/null 2>&1; then
  echo "Redis is not running. Starting Redis..."
  redis-server --daemonize yes
  sleep 1
  if ! redis-cli ping > /dev/null 2>&1; then
    echo "Failed to start Redis. Please start Redis manually."
    exit 1
  fi
  echo "Redis started successfully."
else
  echo "Redis is already running."
fi

# Start the Celery worker in the background
echo "Starting Celery worker with concurrency $WORKER_CONCURRENCY..."
python -m src.api.run_api $COMMON_ARGS --worker --worker-concurrency $WORKER_CONCURRENCY &
WORKER_PID=$!

# Wait for the worker to start
sleep 2

# Start the API server
echo "Starting API server on $API_HOST:$API_PORT..."
python -m src.api.run_api $COMMON_ARGS --host $API_HOST --port $API_PORT

# Clean up
kill $WORKER_PID