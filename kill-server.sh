#!/usr/bin/env bash

PORT=9092

if [ -z "$PORT" ]; then
  echo "❌ Usage: $0 <port>"
  exit 1
fi

PID=$(lsof -ti tcp:$PORT)

if [ -z "$PID" ]; then
  echo "✅ No process found running on port $PORT"
  exit 0
fi

echo "⚠️  Killing process(es) on port $PORT: $PID"
kill -9 $PID

echo "✅ Done"
