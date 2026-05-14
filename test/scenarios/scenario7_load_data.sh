#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-cloudberry-test}"
CLUSTER="${CLUSTER:-scenario1-cluster}"
COORDINATOR="${CLUSTER}-coordinator-0"

echo "=== Scenario 7: Loading test data ==="
echo "Namespace: $NAMESPACE"
echo "Cluster: $CLUSTER"
echo "Coordinator: $COORDINATOR"

# Copy SQL file to pod
kubectl cp test/scenarios/scenario7_load_data.sql \
    "${NAMESPACE}/${COORDINATOR}:/tmp/scenario7_load_data.sql" \
    -c cloudberry

# Execute SQL
kubectl exec "$COORDINATOR" -n "$NAMESPACE" -c cloudberry -- \
    psql -U gpadmin -d mydb -f /tmp/scenario7_load_data.sql

# Verify
echo ""
echo "=== Verification ==="
kubectl exec "$COORDINATOR" -n "$NAMESPACE" -c cloudberry -- \
    psql -U gpadmin -d mydb -c "
SELECT tablename as table_name,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
       (SELECT count(*) FROM information_schema.columns c WHERE c.table_name = t.tablename AND c.table_schema = 'public') as columns,
       obj_description((schemaname||'.'||tablename)::regclass) as distribution
FROM pg_tables t
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"

echo ""
echo "=== Row counts ==="
kubectl exec "$COORDINATOR" -n "$NAMESPACE" -c cloudberry -- \
    psql -U gpadmin -d mydb -c "
SELECT 'customers' as table_name, count(*) as rows FROM customers
UNION ALL SELECT 'orders', count(*) FROM orders
UNION ALL SELECT 'logs', count(*) FROM logs
UNION ALL SELECT 'audit_log', count(*) FROM audit_log
UNION ALL SELECT 'temp_staging', count(*) FROM temp_staging
ORDER BY table_name;
"

echo ""
echo "=== Index count ==="
kubectl exec "$COORDINATOR" -n "$NAMESPACE" -c cloudberry -- \
    psql -U gpadmin -d mydb -t -c "
SELECT count(*) || ' indexes' FROM pg_indexes WHERE schemaname = 'public';
"

echo ""
echo "=== Total database size ==="
kubectl exec "$COORDINATOR" -n "$NAMESPACE" -c cloudberry -- \
    psql -U gpadmin -d mydb -t -c "SELECT pg_size_pretty(pg_database_size('mydb'));"

echo ""
echo "=== Scenario 7 data loading complete ==="
