#cloud-config
write_files:
  - path: /opt/backstage/.env
    content: |
      S3_ENDPOINT_URL=${s3_endpoint}
      S3_ACCESS_KEY=${s3_access_key}
      S3_SECRET_KEY=${s3_secret_key}
      S3_BUCKET=${s3_bucket}
      DATAVERSE_API_TOKEN=${dataverse_token}
      DATAVERSE_SERVER_URL=${dataverse_url}
      DATASET_PERSISTENT_ID_EU=${dataset_persistent_id_eu}
      HCLOUD_TOKEN=${hcloud_token}
      GITHUB_TOKEN=${github_token}
      PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://postgres@localhost:5432/prefect
    permissions: '0600'

runcmd:
  # Install system deps and uv
  - apt-get update && apt-get install -y curl git jq postgresql
  - |
    # Configure PostgreSQL: trust localhost (ephemeral server, no security needed)
    PG_VER=$(pg_lsclusters -h | awk '{print $1}')
    PG_DIR="/etc/postgresql/$PG_VER/main"
    # Enable TCP on localhost
    sed -i "s/^#listen_addresses = .*/listen_addresses = 'localhost'/" "$PG_DIR/postgresql.conf"
    # Trust all local connections (server self-destructs in ~40 min)
    cat > "$PG_DIR/pg_hba.conf" << 'HBAEOF'
    local all all trust
    host all all 127.0.0.1/32 trust
    host all all ::1/128 trust
    HBAEOF
    systemctl restart postgresql
    pg_isready --host=localhost --timeout=30
    # Create Prefect database
    sudo -u postgres psql -c "CREATE DATABASE prefect;"
    sudo -u postgres psql -d prefect -c "CREATE EXTENSION pg_trgm;"
  - curl -LsSf https://astral.sh/uv/install.sh | sh
  - export PATH="/root/.local/bin:$PATH"
  # Clone repo from GitHub
  - git clone https://github.com/openstage-eu/backstage.git /opt/backstage/repo
  - cp /opt/backstage/.env /opt/backstage/repo/.env
  # Install project deps
  - |
    export PATH="/root/.local/bin:$PATH"
    cd /opt/backstage/repo
    uv sync --extra parsing
  # Run pipeline
  - |
    set -e
    export PATH="/root/.local/bin:$PATH"
    cd /opt/backstage/repo
    set -a && . ./.env && set +a
    PIPELINE_STATUS="success"
    START=$(date +%s)
    for CASE in $(echo "${cases}" | tr ',' ' '); do
      uv run python -m flows.run --case $CASE --steps collect download parse package ${pipeline_args} 2>&1 | tee -a /var/log/backstage-run.log || PIPELINE_STATUS="failure"
    done
    END=$(date +%s)
    echo "Pipeline finished in $((END - START)) seconds with status: $PIPELINE_STATUS" | tee -a /var/log/backstage-run.log
  # Upload log to S3
  - |
    export PATH="/root/.local/bin:$PATH"
    cd /opt/backstage/repo
    set -a && . ./.env && set +a
    uv run python -c "from backstage.utils.s3 import upload; upload('/var/log/backstage-run.log', 'logs/$(date +%Y-%m-%d)/run.log')" || true
  # Report back to GitHub (if token is set)
  - |
    . /opt/backstage/.env
    if [ -n "$GITHUB_TOKEN" ]; then
      curl -s -X POST \
        -H "Authorization: token $GITHUB_TOKEN" \
        -H "Accept: application/vnd.github+json" \
        "https://api.github.com/repos/openstage-eu/backstage/dispatches" \
        -d "{\"event_type\": \"pipeline-complete\", \"client_payload\": {\"status\": \"$PIPELINE_STATUS\", \"date\": \"$(date -Iseconds)\"}}" || true
    fi
  # Self-destruct
  - |
    . /opt/backstage/.env
    SERVER_ID=$(curl -s http://169.254.169.254/hetzner/v1/metadata/instance-id)
    curl -s -X DELETE \
      -H "Authorization: Bearer $HCLOUD_TOKEN" \
      "https://api.hetzner.cloud/v1/servers/$SERVER_ID"
