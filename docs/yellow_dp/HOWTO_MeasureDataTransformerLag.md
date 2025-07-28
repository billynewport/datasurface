# HOWTO: Measure DataTransformer Lag

This document provides a comprehensive guide for measuring and evaluating DataTransformer lag in the YellowDataPlatform environment. DataTransformer lag is the delay between source data changes and processed masked data appearing in the final views.

## Understanding Transformer Lag

Transformer lag occurs because:
1. **Source data grows continuously** (via data simulator or real sources)
2. **DataTransformers process data in batches** (not real-time)
3. **Ingestion jobs have processing time** (copying from staging to merge tables)
4. **Views reflect the latest committed batches** (not in-progress work)

**Expected lag patterns:**
- **DataTransformer output**: Should be 90-95% current with source
- **Masked views**: May lag 10-25% behind source during high growth periods
- **Processing time**: Should remain consistent or improve as dataset grows

## Step 1: Check Current Record Counts

```bash
# Get comprehensive comparison of all datasets
kubectl exec deployment/yellowlive-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -d datasurface_merge -c \"
SELECT 
  'Live Customers (Source)' as dataset, 
  COUNT(*) as current_count 
FROM yellowlive_consumer1_livedsg_store1_customers_view_live
UNION ALL
SELECT 
  'YellowLive Masked View' as dataset, 
  COUNT(*) as current_count 
FROM yellowlive_consumer1_livedsg_maskedcustomers_customers_view_94f
UNION ALL  
SELECT 
  'YellowForensic Masked View' as dataset, 
  COUNT(*) as current_count 
FROM yellowforensic_consumer1_livedsg_maskedcustomers_customers__1e6
ORDER BY current_count DESC;
\""

# Check DataTransformer output tables
kubectl exec deployment/yellowlive-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -d datasurface_merge -c \"
SELECT 
  'YellowLive DT Output' as dataset, 
  COUNT(*) as current_count 
FROM yellowlive_dt_maskedcustomers_customers
UNION ALL
SELECT 
  'YellowForensic DT Output' as dataset, 
  COUNT(*) as current_count 
FROM yellowforensic_dt_maskedcustomers_customers;
\""
```

## Step 2: Analyze Batch Processing Status

```bash
# Check YellowLive batch processing
kubectl exec deployment/yellowlive-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -d datasurface_merge -c \"
SELECT 
  'YellowLive' as platform,
  key,
  batch_id, 
  batch_status
FROM yellowlive_batch_metrics 
WHERE key = 'MaskedCustomers' 
ORDER BY batch_id DESC LIMIT 3;
\""

# Check YellowForensic batch processing
kubectl exec deployment/yellowlive-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -d datasurface_merge -c \"
SELECT 
  'YellowForensic' as platform,
  key,
  batch_id, 
  batch_status
FROM yellowforensic_batch_metrics 
WHERE key = 'MaskedCustomers' 
ORDER BY batch_id DESC LIMIT 3;
\""
```

## Step 3: Monitor Execution Performance

```bash
# Extract recent execution times for forensic ingestion jobs
kubectl logs -n ns-yellow-starter deployment/airflow-scheduler --tail=2000 | \
  grep "TaskInstance Finished.*yellowforensic.*output_ingestion_job" | \
  tail -5

# Get execution duration trends
kubectl logs -n ns-yellow-starter deployment/airflow-scheduler --tail=2000 | \
  grep "TaskInstance Finished.*yellowforensic.*output_ingestion_job" | \
  grep -o "run_duration=[0-9.]*" | \
  sed 's/run_duration=//' | \
  sort -n
```

## Step 4: Calculate Lag Metrics

**Lag Calculation Formula:**
```
Source Count: 664 records
YellowLive Masked: 572 records
YellowForensic Masked: 527 records

YellowLive Lag: (664-572)/664 = 14% lag
YellowForensic Lag: (664-527)/664 = 21% lag
```

**Performance Metrics:**
```
Dataset Growth: 316 → 664 records (110% increase)
Execution Time: 5.0 → 5.18-5.55 seconds (3-11% increase)
Throughput: ~63 → ~120 records/second (100% improvement)
```

## Step 5: Evaluate Lag Acceptability

**✅ ACCEPTABLE LAG INDICATORS:**
- **DataTransformer output**: 90-95% current with source
- **View lag**: 10-25% behind source during high growth
- **Execution time**: Consistent or sub-linear scaling
- **Batch progression**: Active processing with committed batches
- **Throughput**: Improving or maintaining performance

**❌ UNACCEPTABLE LAG INDICATORS:**
- **DataTransformer output**: <80% current with source
- **View lag**: >50% behind source consistently
- **Execution time**: Linear or exponential growth with dataset size
- **Batch progression**: Stuck batches or failed processing
- **Throughput**: Declining performance over time

## Step 6: Performance Trend Analysis

**Excellent Performance Pattern:**
```
Dataset Size: 316 → 664 records (+110%)
Execution Time: 5.0 → 5.18-5.55 seconds (+3-11%)
Performance: Sub-linear scaling (optimal)
Throughput: 63 → 120 records/second (+100%)
```

**Key Performance Indicators:**
- **Sub-linear scaling**: Dataset doubles, time increases minimally
- **Consistent execution**: Tight variance in run times (5.18-5.55s range)
- **High throughput**: Processing 120+ records per second
- **Active processing**: Regular batch progression and completion

## Troubleshooting Performance Issues

**Issue: High lag (>50%) with stuck batches**
```bash
# Check for failed jobs
kubectl logs -n ns-yellow-starter deployment/airflow-scheduler --tail=100 | grep -i error

# Verify factory DAG template is correct
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- cat /opt/airflow/dags/yellowforensic_datatransformer_factory_dag.py | grep -A 10 -B 5 "determine_next_action"
```

**Issue: Declining performance over time**
```bash
# Check for resource constraints
kubectl top pods -n ns-yellow-starter

# Monitor database performance
kubectl exec deployment/yellowlive-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -d datasurface_merge -c \"SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables WHERE schemaname LIKE 'yellow%' ORDER BY n_tup_ins DESC;\""
```

**Issue: DataTransformer not keeping up**
```bash
# Check DataTransformer execution logs
kubectl logs -n ns-yellow-starter deployment/airflow-scheduler --tail=200 | grep -E "yellowforensic.*datatransformer_job.*run_duration|yellowlive.*datatransformer_job.*run_duration"

# Verify DataTransformer output freshness
kubectl exec deployment/yellowlive-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -d datasurface_merge -c \"SELECT COUNT(*) as dt_output_count FROM yellowforensic_dt_maskedcustomers_customers;\""
```

## Success Criteria for Lag Verification

**✅ EXCELLENT PERFORMANCE:**
- DataTransformer output: 90-95% current
- View lag: 10-25% during high growth
- Sub-linear execution time scaling
- Consistent batch progression
- Improving throughput over time

**✅ ACCEPTABLE PERFORMANCE:**
- DataTransformer output: 80-90% current
- View lag: 25-40% during high growth
- Linear execution time scaling
- Regular batch completion
- Stable throughput

**❌ NEEDS ATTENTION:**
- DataTransformer output: <80% current
- View lag: >40% consistently
- Exponential execution time growth
- Failed or stuck batches
- Declining throughput

## Continuous Monitoring

For production environments, set up regular monitoring:

```bash
# Create a monitoring script
cat > monitor_lag.sh << 'EOF'
#!/bin/bash
echo "=== YellowDataPlatform Lag Monitor ==="
echo "Timestamp: $(date)"
echo ""

# Get current counts
SOURCE_COUNT=$(kubectl exec deployment/yellowlive-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -d datasurface_merge -c \"SELECT COUNT(*) FROM yellowlive_consumer1_livedsg_store1_customers_view_live;\" | tail -1 | tr -d ' '")
LIVE_MASKED=$(kubectl exec deployment/yellowlive-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -d datasurface_merge -c \"SELECT COUNT(*) FROM yellowlive_consumer1_livedsg_maskedcustomers_customers_view_94f;\" | tail -1 | tr -d ' '")
FORENSIC_MASKED=$(kubectl exec deployment/yellowlive-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -d datasurface_merge -c \"SELECT COUNT(*) FROM yellowforensic_consumer1_livedsg_maskedcustomers_customers__1e6;\" | tail -1 | tr -d ' '")

# Calculate lag percentages
LIVE_LAG=$(( (SOURCE_COUNT - LIVE_MASKED) * 100 / SOURCE_COUNT ))
FORENSIC_LAG=$(( (SOURCE_COUNT - FORENSIC_MASKED) * 100 / SOURCE_COUNT ))

echo "Source Records: $SOURCE_COUNT"
echo "YellowLive Masked: $LIVE_MASKED (${LIVE_LAG}% lag)"
echo "YellowForensic Masked: $FORENSIC_MASKED (${FORENSIC_LAG}% lag)"
echo ""

# Performance assessment
if [ $LIVE_LAG -le 25 ] && [ $FORENSIC_LAG -le 25 ]; then
    echo "✅ EXCELLENT: Both platforms within acceptable lag"
elif [ $LIVE_LAG -le 40 ] && [ $FORENSIC_LAG -le 40 ]; then
    echo "⚠️  ACCEPTABLE: Lag within operational limits"
else
    echo "❌ ATTENTION: High lag detected - investigate"
fi
EOF

chmod +x monitor_lag.sh

# Run monitoring every 5 minutes
watch -n 300 ./monitor_lag.sh
```

## Example Output Analysis

**Typical Good Performance:**
```
Source Records: 664
YellowLive Masked: 572 (14% lag)
YellowForensic Masked: 527 (21% lag)

✅ EXCELLENT: Both platforms within acceptable lag
```

**Performance Metrics:**
- **DataTransformer Output**: 619 records (93% current)
- **Execution Times**: 5.18-5.55 seconds (consistent)
- **Throughput**: 120+ records/second
- **Batch Status**: Active processing with committed batches

## Key Performance Insights

1. **Sub-linear Scaling**: Dataset doubled (110% growth) but execution time increased only 3-11%
2. **Consistent Performance**: Tight variance in execution times indicates stable processing
3. **High Throughput**: Processing 120+ records per second demonstrates efficient batch processing
4. **Active Pipeline**: Regular batch progression shows healthy pipeline operation

This monitoring approach ensures that transformer lag remains within acceptable parameters and provides early warning of performance issues that could impact data pipeline reliability. 