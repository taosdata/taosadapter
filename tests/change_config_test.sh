#!/bin/bash
set -e
cat >/etc/taos/taosadapter.toml << 'EOF'
EOF

sleep 2

response_code=$(curl -u root:taosdata -s -o /dev/null -w "%{http_code}" localhost:6041/rest/sql -d "show databases")
if [ "$response_code" -ne 200 ]; then
    echo "Failed to connect to TDengine RESTful API. Response code: $response_code"
    exit 1
else
    echo "Successfully connected to TDengine RESTful API. Response code: $response_code"
fi

cat >/etc/taos/taosadapter.toml << 'EOF'
rejectQuerySqlRegex = ['(?i)^show\s+databases.*']
[log]
level = "debug"
EOF

sleep 2
response_code=$(curl -u root:taosdata -s -o /dev/null -w "%{http_code}" localhost:6041/rest/sql -d "show databases")
if [ "$response_code" -ne 403 ]; then
    echo "Configuration change did not take effect as expected. Response code: $response_code"
    exit 1
else
    echo "Configuration change took effect successfully. Response code: $response_code"
fi