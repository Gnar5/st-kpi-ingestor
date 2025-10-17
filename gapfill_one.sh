# gapfill_one.sh  (save next to your repo, not in src/)
#!/bin/bash
set -euo pipefail

SERVICE="https://st-kpi-ingestor-999875365235.us-central1.run.app"
BU="${1:?Usage: ./gapfill_one.sh 'BU-NAME'  [DAYS] }"
DAYS="${2:-400}"
TZ="America/Phoenix"

echo "Finding missing dates for $BU (last $DAYS days)…"
bq query --use_legacy_sql=false --format=csv "
WITH cal AS (
  SELECT GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE('$TZ'), INTERVAL ${DAYS}-1 DAY),
                             CURRENT_DATE('$TZ')) AS d
), days AS (
  SELECT day AS d FROM UNNEST((SELECT d FROM cal)) AS day
), have AS (
  SELECT DISTINCT event_date AS d
  FROM \`kpi-auto-471020.st_raw.raw_daily_wbr_v2\`
  WHERE bu_name = '${BU}'
    AND event_date BETWEEN DATE_SUB(CURRENT_DATE('$TZ'), INTERVAL ${DAYS}-1 DAY)
                        AND CURRENT_DATE('$TZ')
)
SELECT d FROM days
EXCEPT DISTINCT
SELECT d FROM have
ORDER BY d
" | tail -n +2 > missing_${BU// /_}.txt

COUNT=$(wc -l < missing_${BU// /_}.txt | tr -d ' ')
echo "Missing days for $BU: $COUNT"

i=0
while read -r D; do
  [[ -z "$D" ]] && continue
  i=$((i+1))
  echo "[$i/$COUNT] Ingesting $BU $D"
  curl -s "$SERVICE/ingest/daily_wbr?bu=$(printf %s "$BU")&days=1&to=$D" >/dev/null || true
  sleep 3
done < missing_${BU// /_}.txt

echo "Gap fill complete for $BU."
