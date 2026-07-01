#!/usr/bin/env bash
# preflight.sh — verify the local machine is ready to drive the
# metronome AWS evaluation. Installs aws-cli if missing, then runs the
# four account-readiness checks and prints a digest.
#
# Run from anywhere:  ./preflight.sh
#
# Re-runnable: idempotent; safe to invoke multiple times.

set -uo pipefail

# ── Constants for this eval ──────────────────────────────────────────
REGION=us-west-1
KEY_NAME=metronome-aws
INSTANCE_TYPE=c6in.2xlarge
QUOTA_CODE=L-34B43A08    # "All Standard (A, C, D, H, I, M, R, T, Z) Spot Instance Requests"
NEED_VCPUS=64            # 7 servers + 1 driver, 8 vCPU each
PROFILE=${AWS_PROFILE:-default}

# ── Pretty-printing helpers ─────────────────────────────────────────
say()  { printf "\n\033[1;34m▸\033[0m %s\n" "$*"; }
ok()   { printf "  \033[1;32m✓\033[0m %s\n" "$*"; }
warn() { printf "  \033[1;33m!\033[0m %s\n" "$*"; }
fail() { printf "  \033[1;31m✗\033[0m %s\n" "$*"; }
die()  { fail "$*"; exit 1; }

# ── (a) Install aws-cli if needed ───────────────────────────────────
ensure_aws_cli() {
  if command -v aws >/dev/null 2>&1; then
    ok "aws-cli present: $(aws --version 2>&1 | head -1)"
    return
  fi
  say "aws-cli not found — installing"
  case "$(uname -s)" in
    Darwin)
      if command -v brew >/dev/null 2>&1; then
        brew install awscli || die "brew install awscli failed"
      else
        warn "Homebrew not found; falling back to AWS official .pkg installer (will prompt for sudo)"
        local tmpd; tmpd=$(mktemp -d)
        curl -fsSL "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "$tmpd/AWSCLIV2.pkg" \
          || die "download AWSCLIV2.pkg failed"
        sudo installer -pkg "$tmpd/AWSCLIV2.pkg" -target / || die "pkg install failed"
        rm -rf "$tmpd"
      fi
      ;;
    Linux)
      local arch awsarch tmpd
      arch=$(uname -m)
      case "$arch" in
        x86_64)        awsarch=x86_64 ;;
        aarch64|arm64) awsarch=aarch64 ;;
        *) die "unsupported arch $arch" ;;
      esac
      tmpd=$(mktemp -d); pushd "$tmpd" >/dev/null || die
      curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${awsarch}.zip" -o awscliv2.zip \
        || die "download awscli zip failed"
      unzip -q awscliv2.zip || die "unzip failed"
      sudo ./aws/install || die "aws install failed"
      popd >/dev/null; rm -rf "$tmpd"
      ;;
    *) die "unsupported OS: $(uname -s)" ;;
  esac
  ok "aws-cli installed: $(aws --version 2>&1 | head -1)"
}

# ── (b) Verify credentials are usable ───────────────────────────────
ensure_credentials() {
  if aws --profile "$PROFILE" sts get-caller-identity >/dev/null 2>&1; then
    return
  fi
  cat <<EOF >&2

  AWS credentials missing or invalid for profile '$PROFILE'.

  Options:
    • aws configure --profile $PROFILE          (interactive, recommended)
    • export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…
    • aws sso login --profile $PROFILE          (if you use SSO)

  Re-run this script after credentials are in place.
EOF
  exit 1
}

# ── Run ─────────────────────────────────────────────────────────────
ensure_aws_cli
ensure_credentials

printf "\n========================================================\n"
printf "  AWS preflight report  (region=%s, profile=%s)\n" "$REGION" "$PROFILE"
printf "========================================================\n"

# Check 1/4: caller identity
say "Check 1/4 — caller identity"
aws --profile "$PROFILE" --region "$REGION" sts get-caller-identity --output table \
  || die "sts get-caller-identity failed"

# Check 2/4: spot price history
say "Check 2/4 — spot price history for $INSTANCE_TYPE (Linux)"
SPOT_OUT=$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-spot-price-history \
            --instance-types "$INSTANCE_TYPE" \
            --product-descriptions "Linux/UNIX" \
            --max-items 5 \
            --query 'SpotPriceHistory[].[AvailabilityZone,SpotPrice,Timestamp]' \
            --output table 2>&1) || die "describe-spot-price-history failed: $SPOT_OUT"
echo "$SPOT_OUT"

# Pull current min spot price for reporting.
MIN_SPOT=$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-spot-price-history \
            --instance-types "$INSTANCE_TYPE" \
            --product-descriptions "Linux/UNIX" \
            --max-items 20 \
            --query 'min_by(SpotPriceHistory,&SpotPrice).SpotPrice' \
            --output text 2>/dev/null) || MIN_SPOT="?"
ok "min observed Spot price: \$${MIN_SPOT}/hr for $INSTANCE_TYPE"

# Check 3/4: spot vCPU quota
say "Check 3/4 — Spot vCPU quota (need >= $NEED_VCPUS; recommend 128)"
QUOTA_JSON=$(aws --profile "$PROFILE" --region "$REGION" service-quotas get-service-quota \
              --service-code ec2 --quota-code "$QUOTA_CODE" --output json 2>&1) \
  || die "service-quotas API failed:
$QUOTA_JSON"
QUOTA_VAL=$(printf '%s' "$QUOTA_JSON" \
              | python3 -c "import json,sys;print(int(json.load(sys.stdin)['Quota']['Value']))" \
              2>/dev/null) || QUOTA_VAL=0
echo "    Current Spot vCPU quota: $QUOTA_VAL"
if [[ "$QUOTA_VAL" -lt "$NEED_VCPUS" ]]; then
  fail "TOO LOW. Need at least $NEED_VCPUS (recommend 128)."
  echo "    Request increase here:"
  echo "      https://${REGION}.console.aws.amazon.com/servicequotas/home/services/ec2/quotas/${QUOTA_CODE}"
  QUOTA_OK=0
else
  ok "sufficient"
  QUOTA_OK=1
fi

# Check 4/4: key pair presence
say "Check 4/4 — key pair '$KEY_NAME' in $REGION"
if aws --profile "$PROFILE" --region "$REGION" ec2 describe-key-pairs \
      --key-names "$KEY_NAME" >/dev/null 2>&1; then
  ok "present"
  KEY_OK=1
else
  fail "not found in $REGION"
  echo "    Import it from the local private key:"
  echo
  echo "      ssh-keygen -y -f ~/.ssh/${KEY_NAME}.pem | \\"
  echo "        aws --profile $PROFILE --region $REGION ec2 import-key-pair \\"
  echo "          --key-name $KEY_NAME --public-key-material fileb:///dev/stdin"
  KEY_OK=0
fi

# ── Final digest ────────────────────────────────────────────────────
printf "\n──────────────── summary ────────────────\n"
ok "aws-cli OK, credentials OK"
[[ "$QUOTA_OK" -eq 1 ]] && ok "spot quota OK ($QUOTA_VAL vCPU)" || fail "spot quota TOO LOW ($QUOTA_VAL vCPU)"
[[ "$KEY_OK"   -eq 1 ]] && ok "key pair OK"                       || fail "key pair MISSING"
echo
if [[ "$QUOTA_OK$KEY_OK" == "11" ]]; then
  ok "all green — safe to generate Terraform"
else
  fail "fix the items above before proceeding"
  exit 2
fi
