#!/bin/bash
#=====================================================================
# run_nightly_batch.sh — Nightly batch runner
# Sybase ASE — isql-based job scheduler wrapper
#
# Called by cron at 02:00 nightly:
#   0 2 * * * /opt/sybase/scripts/run_nightly_batch.sh >> /var/log/sybase/nightly.log 2>&1
#
# In the SQL Server world, this becomes a SQL Server Agent Job
# or an Azure Data Factory pipeline.
#=====================================================================

SYBASE_SERVER="${SYBASE_SERVER:-LOAN_PROD}"
SYBASE_DB="${SYBASE_DB:-loan_servicing}"
SYBASE_USER="${SYBASE_USER:-sa_batch}"
LOG_DIR="/var/log/sybase"
DATE=$(date +%Y%m%d)

echo "=== Nightly Batch Start: $(date) ==="

# Step 1: Nightly accrual
echo "Running sp_nightly_accrual..."
if ! isql -U ${SYBASE_USER} -P ${SYBASE_PWD} -S ${SYBASE_SERVER} -D ${SYBASE_DB} <<EOF
EXEC dbo.sp_nightly_accrual @processing_date = GETDATE()
go
EOF
then
    echo "FAILED: sp_nightly_accrual"
    exit 1
fi

# Step 2: Apply late fees
echo "Running sp_apply_late_fees..."
if ! isql -U ${SYBASE_USER} -P ${SYBASE_PWD} -S ${SYBASE_SERVER} -D ${SYBASE_DB} <<EOF
EXEC dbo.sp_apply_late_fees @cutoff_date = GETDATE()
go
EOF
then
    echo "FAILED: sp_apply_late_fees"
    exit 1
fi

# Step 3: EOD reconciliation
echo "Running sp_end_of_day_reconciliation..."
if ! isql -U ${SYBASE_USER} -P ${SYBASE_PWD} -S ${SYBASE_SERVER} -D ${SYBASE_DB} <<EOF
EXEC dbo.sp_end_of_day_reconciliation
go
EOF
then
    echo "FAILED: sp_end_of_day_reconciliation — CHECK RECON RESULTS"
    exit 1
fi

echo "=== Nightly Batch Complete: $(date) ==="
