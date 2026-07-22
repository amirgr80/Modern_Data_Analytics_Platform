#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$HOME/projects/modern_data_analytics_platform"
DAG_ID="silver_behavioral_etl_v2"

BACKFILL_DATES=(
  "2026-07-14"
  "2026-07-15"
  "2026-07-16"
  "2026-07-17"
)

POLL_SECONDS="${POLL_SECONDS:-30}"
MAX_POLLS="${MAX_POLLS:-360}"

LOG_DIR="$HOME/.config/modern_data_analytics_platform/backfill-logs"
LOG_FILE="$LOG_DIR/behavioral-backfill-$(date -u +%Y%m%dT%H%M%SZ).log"

mkdir -p "$LOG_DIR"
cd "$PROJECT_DIR"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "=== BEHAVIORAL BACKFILL CONTROLLER ==="
echo "started_at=$(date -Iseconds)"
echo "project_dir=$PROJECT_DIR"
echo "dag_id=$DAG_ID"
echo "log_file=$LOG_FILE"
echo "dates=${BACKFILL_DATES[*]}"

db_scalar() {
  local sql="$1"

  docker compose exec -T \
    -e SQL="$sql" \
    postgres bash -lc '
      psql \
        -U "$POSTGRES_USER" \
        -d "${POSTGRES_DB:-airflow}" \
        -Atc "$SQL"
    '
}

pause_dag() {
  echo "Pausing $DAG_ID for safety..."

  docker compose exec -T airflow-apiserver \
    airflow dags pause "$DAG_ID" >/dev/null 2>&1 || true
}

fail_controller() {
  local message="$1"

  echo
  echo "BACKFILL_CONTROLLER=FAILED"
  echo "reason=$message"
  echo "failed_at=$(date -Iseconds)"

  pause_dag
  exit 1
}

echo
echo "=== GLOBAL SAFETY CHECKS ==="

ACTIVE_RUNS="$(
  db_scalar "
    SELECT COUNT(*)
    FROM dag_run
    WHERE dag_id = '$DAG_ID'
      AND state IN ('queued', 'running');
  "
)"

echo "active_runs=$ACTIVE_RUNS"

if [[ "$ACTIVE_RUNS" != "0" ]]; then
  fail_controller "An active Behavioral run already exists."
fi

DETAILS="$(
  docker compose exec -T airflow-apiserver \
    airflow dags details "$DAG_ID"
)"

printf '%s\n' "$DETAILS" |
grep -E \
'^(is_paused|timetable_summary|has_import_errors)[[:space:]]*\|' \
|| true

if ! printf '%s\n' "$DETAILS" |
     grep -Eq \
     '^is_paused[[:space:]]*\|[[:space:]]*False'; then
  fail_controller "DAG is paused."
fi

if ! printf '%s\n' "$DETAILS" |
     grep -Eq \
     '^timetable_summary[[:space:]]*\|[[:space:]]*None'; then
  fail_controller "Schedule isolation is not active."
fi

if ! printf '%s\n' "$DETAILS" |
     grep -Eq \
     '^has_import_errors[[:space:]]*\|[[:space:]]*False'; then
  fail_controller "DAG has import errors."
fi

if ! grep -Fq \
     -- "--executor-memory '4g'" \
     workflow/dags/silver_behavioral_dag.py; then
  fail_controller "Executor memory 4g is not configured."
fi

if ! grep -Fq \
     -- "--driver-memory '2g'" \
     workflow/dags/silver_behavioral_dag.py; then
  fail_controller "Driver memory 2g is not configured."
fi

echo "global_safety_checks=PASS"

for PROCESS_DATE in "${BACKFILL_DATES[@]}"; do
  echo
  echo "=================================================="
  echo "PROCESS_DATE=$PROCESS_DATE"
  echo "=================================================="

  PREVIOUS_SUCCESS="$(
    db_scalar "
      SELECT COUNT(*)
      FROM dag_run
      WHERE dag_id = '$DAG_ID'
        AND state = 'success'
        AND (
          conf::jsonb ->> 'execution_date'
        ) = '$PROCESS_DATE';
    "
  )"

  if [[ "$PREVIOUS_SUCCESS" != "0" ]]; then
    echo "date_status=ALREADY_SUCCEEDED"
    echo "date_action=SKIPPED"
    continue
  fi

  ACTIVE_RUNS="$(
    db_scalar "
      SELECT COUNT(*)
      FROM dag_run
      WHERE dag_id = '$DAG_ID'
        AND state IN ('queued', 'running');
    "
  )"

  if [[ "$ACTIVE_RUNS" != "0" ]]; then
    fail_controller \
      "Another run appeared before triggering $PROCESS_DATE."
  fi

  RUN_ID="manual__silver_behavioral_auto_backfill_${PROCESS_DATE}_$(date -u +%Y%m%dT%H%M%SZ)"

  echo "run_id=$RUN_ID"
  echo "triggered_at=$(date -Iseconds)"

  docker compose exec -T airflow-apiserver \
    airflow dags trigger \
      -r "$RUN_ID" \
      -c "{\"execution_date\":\"$PROCESS_DATE\"}" \
      "$DAG_ID"

  RUN_FINISHED="False"

  for POLL in $(seq 1 "$MAX_POLLS"); do
    DAG_STATE="$(
      db_scalar "
        SELECT COALESCE(
          (
            SELECT state
            FROM dag_run
            WHERE dag_id = '$DAG_ID'
              AND run_id = '$RUN_ID'
          ),
          'not_found'
        );
      "
    )"

    TASK_STATE="$(
      db_scalar "
        SELECT
          COALESCE(state, 'none')
          || '|try='
          || try_number
        FROM task_instance
        WHERE dag_id = '$DAG_ID'
          AND run_id = '$RUN_ID'
          AND task_id = 'run_silver_behavioral_job_v2';
      "
    )"

    echo \
      "$(date -Iseconds) date=$PROCESS_DATE poll=$POLL dag_state=$DAG_STATE task=$TASK_STATE"

    case "$DAG_STATE" in
      success)
        RUN_FINISHED="True"
        echo "date_result=SUCCESS"
        echo "completed_at=$(date -Iseconds)"
        break
        ;;

      failed)
        fail_controller \
          "Backfill failed for $PROCESS_DATE; run_id=$RUN_ID"
        ;;

      not_found)
        fail_controller \
          "Triggered run was not found: $RUN_ID"
        ;;
    esac

    sleep "$POLL_SECONDS"
  done

  if [[ "$RUN_FINISHED" != "True" ]]; then
    fail_controller \
      "Timed out waiting for $PROCESS_DATE; run_id=$RUN_ID"
  fi

  NON_SUCCESS_TASKS="$(
    db_scalar "
      SELECT COUNT(*)
      FROM task_instance
      WHERE dag_id = '$DAG_ID'
        AND run_id = '$RUN_ID'
        AND state IS DISTINCT FROM 'success';
    "
  )"

  echo "non_success_tasks=$NON_SUCCESS_TASKS"

  if [[ "$NON_SUCCESS_TASKS" != "0" ]]; then
    fail_controller \
      "DAG succeeded but one or more tasks were not successful."
  fi

  sleep 10
done

echo
echo "=== ALL REQUESTED DATES COMPLETED ==="

pause_dag

echo "finished_at=$(date -Iseconds)"
echo "BACKFILL_CONTROLLER=SUCCESS"
echo "DAG_PAUSED_AFTER_BACKFILL=True"
echo "FINAL_LOG=$LOG_FILE"
