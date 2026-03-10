#!/usr/bin/env bash
# Launch a backstage pipeline run on Hetzner Cloud.
#
# Prerequisites:
#   - .env with S3 credentials and HCLOUD_TOKEN
#   - Terraform installed
#   - uv installed
#
# Usage:
#   ./deploy/launch.sh                                          # default: cx53, case=eu
#   ./deploy/launch.sh --server cpx51                           # override server type
#   ./deploy/launch.sh --pipeline-args "--download-mode incremental"  # extra args to flows.run

set -euo pipefail
cd "$(dirname "$0")/.."

# Load env
set -a && source .env && set +a

SERVER_TYPE=""
PIPELINE_ARGS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server)
      SERVER_TYPE="${2:?Usage: $0 --server <type>}"
      shift 2
      ;;
    --pipeline-args)
      PIPELINE_ARGS="${2:?Usage: $0 --pipeline-args '<args>'}"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

echo "==> Packaging code tarball..."
tar czf /tmp/backstage.tar.gz \
  --exclude='.venv' \
  --exclude='__pycache__' \
  --exclude='.git' \
  --exclude='*.pyc' \
  --exclude='.env' \
  --exclude='deploy/terraform/.terraform' \
  --exclude='deploy/terraform/*.tfstate*' \
  -C "$(pwd)" .

echo "==> Uploading tarball to S3..."
uv run python -c "
from backstage.utils.s3 import upload
upload('/tmp/backstage.tar.gz', 'deploy/backstage.tar.gz')
print('Uploaded deploy/backstage.tar.gz')
"

echo "==> Initializing Terraform..."
cd deploy/terraform
terraform init -input=false

echo "==> Creating server..."
terraform apply -auto-approve -input=false \
  -var="hcloud_token=$HCLOUD_TOKEN" \
  -var="s3_endpoint=$S3_ENDPOINT_URL" \
  -var="s3_access_key=$S3_ACCESS_KEY" \
  -var="s3_secret_key=$S3_SECRET_KEY" \
  -var="s3_bucket=${S3_BUCKET:-openstage}" \
  ${GITHUB_TOKEN:+-var="github_token=$GITHUB_TOKEN"} \
  ${SERVER_TYPE:+-var="server_type=$SERVER_TYPE"} \
  ${PIPELINE_ARGS:+-var="pipeline_args=$PIPELINE_ARGS"}

echo ""
echo "==> Server launched. It will:"
echo "    1. Install uv and dependencies"
echo "    2. Run the pipeline (collect, download, parse, package)"
echo "    3. Upload log to S3 at logs/<date>/run.log"
echo "    4. Self-destruct"
echo ""
echo "    Monitor via: hcloud server list"
echo "    Logs:        hcloud server ssh <name> 'tail -f /var/log/backstage-run.log'"
echo "    Or wait for:  S3 logs/<date>/run.log"
