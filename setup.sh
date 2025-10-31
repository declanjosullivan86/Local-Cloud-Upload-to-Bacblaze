#!/usr/bin/env bash
#
# upload_with_audit.sh
#
# Upload files from local or cloud stage to SSH / HTTP / S3 with audit logging
# and per-file status files for GUI frontends to read/observe progress.
#
# Dependencies (optional): pv, aws, curl, ssh, scp, sha256sum, flock
#
# Usage:
#   ./upload_with_audit.sh --source /path/to/file_or_dir \
#                          --target ssh:user@host:/remote/dir \
#                          --audit-log ./upload_audit.log \
#                          --status-dir /tmp/upload_status
#
# Target forms:
#   ssh:user@host:/path/to/dir-or-file
#   http:https://example.com/upload/ (uses PUT)
#   s3:bucket/path/prefix/            (uses `aws s3 cp - s3://...`)
#
# The script will create per-file status JSON files under --status-dir
# and append audit records (JSON Lines) to the audit log file.

set -euo pipefail
IFS=$'\n\t'

# -------- default config --------
AUDIT_LOG="./upload_audit.log"
STATUS_DIR="/tmp/upload_status"
CONCURRENCY=1   # not used fully concurrent in this script (simple single-thread)
DRY_RUN=0
VERBOSE=1
# --------------------------------

usage() {
  cat <<EOF
Usage: $0 --source <path> --target <target> [options]

Options:
  --source PATH          Path to local file or directory to upload.
  --target TARGET        Target. One of:
                           ssh:user@host:/remote/path
                           http:https://example.com/upload/
                           s3:bucket/path/prefix/
  --audit-log PATH       Audit log file (JSONL). Default: $AUDIT_LOG
  --status-dir PATH      Directory for per-file status JSON. Default: $STATUS_DIR
  --dry-run              Print actions but do not transfer.
  --quiet                Reduce output.
  -h, --help             Show this help.

Example:
  $0 --source ./build --target s3:my-bucket/artifacts/ --audit-log ./audit.log
EOF
  exit 1
}

# -------- parse args --------
SOURCE=""
TARGET=""
while [[ "${#}" -gt 0 ]]; do
  case "$1" in
    --source) SOURCE="$2"; shift 2;;
    --target) TARGET="$2"; shift 2;;
    --audit-log) AUDIT_LOG="$2"; shift 2;;
    --status-dir) STATUS_DIR="$2"; shift 2;;
    --dry-run) DRY_RUN=1; shift;;
    --quiet) VERBOSE=0; shift;;
    -h|--help) usage;;
    *) echo "Unknown arg: $1"; usage;;
  esac
done

if [[ -z "$SOURCE" || -z "$TARGET" ]]; then
  usage
fi

mkdir -p "$STATUS_DIR"
mkdir -p "$(dirname "$AUDIT_LOG")"

log() {
  if [[ $VERBOSE -eq 1 ]]; then
    echo "$@"
  fi
}

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# atomic append to audit log using flock if available
append_audit() {
  local json="$1"
  if command -v flock >/dev/null 2>&1; then
    # use file descriptor to lock the audit log
    (
      flock -x 200
      printf '%s\n' "$json" >>"$AUDIT_LOG"
    ) 200>"$AUDIT_LOG.lock"
  else
    printf '%s\n' "$json" >>"$AUDIT_LOG"
  fi
}

# write status JSON file for GUI; each file gets basename.status.json
write_status() {
  local file="$1"; shift
  local json="$*"
  local status_file="$STATUS_DIR/$(basename "$file").status.json"
  printf '%s\n' "$json" > "$status_file"
}

# compute sha256 (portable)
compute_sha256() {
  local f="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$f" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$f" | awk '{print $1}'
  else
    echo "unknown"
  fi
}

# transfer a single file
# params: local_file target_type target_spec
# returns: exit code of transfer
transfer_file() {
  local local_file="$1"
  local target_type="$2"
  local target_spec="$3"
  local filename
  filename=$(basename "$local_file")

  local status_file="$STATUS_DIR/$filename.status.json"
  write_status "$local_file" "{\"file\":\"$filename\",\"status\":\"starting\",\"ts\":\"$(timestamp)\"}"

  local size
  size=$(stat -c%s "$local_file" 2>/dev/null || stat -f%z "$local_file" 2>/dev/null || echo 0)
  local sha
  sha=$(compute_sha256 "$local_file")
  local start_ts
  start_ts=$(date +%s)

  # helper to write progress updates (percent 0..100, bytes)
  update_progress() {
    local percent="$1"
    local transferred="$2"
    local msg="${3:-}"
    # safe JSON (minimal escaping)
    printf '{"file":"%s","status":"transferring","percent":%s,"transferred":%s,"size":%s,"msg":"%s","ts":"%s"}\n' \
      "$filename" "$percent" "$transferred" "$size" "$(echo "$msg" | sed 's/"/\\"/g')" "$(timestamp)" > "$status_file"
  }

  # final status writer
  finalize_status() {
    local outcome="$1"
    local exitcode="$2"
    local end_ts
    end_ts=$(date +%s)
    local dur=$((end_ts - start_ts))
    printf '{"file":"%s","status":"%s","exit_code":%d,"duration_s":%d,"sha256":"%s","size":%s,"ts":"%s"}\n' \
      "$filename" "$outcome" "$exitcode" "$dur" "$sha" "$size" "$(timestamp)" > "$status_file"
  }

  if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY-RUN] would transfer $local_file -> $target_type:$target_spec"
    finalize_status "dry-run" 0
    return 0
  fi

  # Choose transfer method and attempt to use pv to emit progress
  local exitcode=1
  case "$target_type" in
    ssh)
      # target_spec is like user@host:/remote/path/prefix/
      # We'll stream via pv into ssh "cat > remote/path/filename"
      remote_base="$target_spec"
      # If remote path ends with / treat as directory
      if [[ "${remote_base: -1}" == "/" ]]; then
        remote_path="${remote_base}${filename}"
      else
        remote_path="$remote_base"
      fi

      if command -v pv >/dev/null 2>&1; then
        # Use pv to emit progress % on stderr (-n)
        # Redirect pv stderr to a process substitution that updates status
        log "Uploading (ssh) $local_file -> $remote_path via ssh (pv available)"
        pv -n "$local_file" 2> >(
          # pv writes numbers 0..1; convert to percent
          while read -r p; do
            pct=$(awk "BEGIN{printf \"%d\", ($p*100)}")
            bytes=$(awk "BEGIN{printf \"%d\", ($p*$size)}")
            update_progress "$pct" "$bytes" ""
          done
        ) | ssh "${remote_path%%:*}" "cat > '${remote_path#*:}'"
        exitcode=${PIPESTATUS[1]:-0}
      else
        log "Uploading (ssh) without pv: $local_file -> $remote_path"
        scp "$local_file" "$remote_path"
        exitcode=$?
      fi
      ;;

    http)
      # target_spec is the URL base or full url; we'll PUT to target_spec/filename if target_spec is a directory
      url="$target_spec"
      if [[ "${url: -1}" == "/" ]]; then
        url="${url}${filename}"
      fi

      if command -v pv >/dev/null 2>&1; then
        log "Uploading (http) $local_file -> $url via curl with pv"
        pv -n "$local_file" 2> >(
          while read -r p; do
            pct=$(awk "BEGIN{printf \"%d\", ($p*100)}")
            bytes=$(awk "BEGIN{printf \"%d\", ($p*$size)}")
            update_progress "$pct" "$bytes" ""
          done
        ) | curl -s -S --fail -T - "$url"
        exitcode=${PIPESTATUS[1]:-0}
      else
        log "Uploading (http) without pv: $local_file -> $url"
        curl -s -S --fail --upload-file "$local_file" "$url"
        exitcode=$?
      fi
      ;;

    s3)
      # target_spec is s3 path prefix: bucket/path/prefix/
      s3path="$target_spec"
      if [[ "${s3path: -1}" == "/" ]]; then
        s3key="${s3path}${filename}"
      else
        s3key="$s3path"
      fi

      if ! command -v aws >/dev/null 2>&1; then
        log "aws CLI not found; cannot upload to s3"
        exitcode=127
      else
        if command -v pv >/dev/null 2>&1; then
          log "Uploading (s3) $local_file -> s3://$s3key using pv | aws s3 cp -"
          pv -n "$local_file" 2> >(
            while read -r p; do
              pct=$(awk "BEGIN{printf \"%d\", ($p*100)}")
              bytes=$(awk "BEGIN{printf \"%d\", ($p*$size)}")
              update_progress "$pct" "$bytes" ""
            done
          ) | aws s3 cp - "s3://$s3key" --only-show-errors
          exitcode=${PIPESTATUS[1]:-0}
        else
          log "Uploading (s3) without pv: $local_file -> s3://$s3key"
          aws s3 cp "$local_file" "s3://$s3key" --only-show-errors
          exitcode=$?
        fi
      fi
      ;;

    *)
      log "Unsupported target type: $target_type"
      exitcode=2
      ;;
  esac

  # finalize
  if [[ $exitcode -eq 0 ]]; then
    finalize_status "success" 0
  else
    finalize_status "failed" $exitcode
  fi

  # Prepare audit JSON
  local end_ts
  end_ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  local duration
  duration=$(( $(date +%s) - start_ts ))
  # Minimal JSON with safe escaping for filename
  local audit_json
  audit_json=$(cat <<EOF
{"file":"$filename","local_path":"$local_file","target_type":"$target_type","target_spec":"$target_spec","size":$size,"sha256":"$sha","start_ts":"$(date -u -d "@$start_ts" +"%Y-%m-%dT%H:%M:%SZ")","end_ts":"$end_ts","duration_s":$duration,"exit_code":$exitcode,"user":"$(whoami)","host":"$(hostname)"}
EOF
)
  append_audit "$audit_json"
  return $exitcode
}

# -------- helper to enumerate files from SOURCE --------
collect_files() {
  local src="$1"
  if [[ -f "$src" ]]; then
    printf '%s\n' "$src"
  elif [[ -d "$src" ]]; then
    # find regular files only
    find "$src" -type f -print
  else
    echo "Source not found: $src" >&2
    return 1
  fi
}

# parse TARGET into type and spec
if [[ "$TARGET" =~ ^ssh: ]]; then
  TTYPE="ssh"; TSPEC="${TARGET#ssh:}"
elif [[ "$TARGET" =~ ^http: || "$TARGET" =~ ^https: ]]; then
  TTYPE="http"; TSPEC="${TARGET#http:}"; TSPEC="http:${TSPEC}"
  # if it was "http:https://..." earlier then above preserves full URL
  # but keep simpler: if TARGET starts with http(s):// then TTYPE=http and TSPEC=TARGET
  if [[ "$TARGET" =~ ^https?:// ]]; then TTYPE="http"; TSPEC="$TARGET"; fi
elif [[ "$TARGET" =~ ^s3: ]]; then
  TTYPE="s3"; TSPEC="${TARGET#s3:}"
else
  echo "Unrecognized target form. Must start with ssh:, http(s)://, or s3:." >&2
  exit 2
fi

# If TSPEC has unintended leading slashes from earlier manipulation, normalize:
TSPEC="${TSPEC#/}"

# iterate files
mapfile -t FILES < <(collect_files "$SOURCE")

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "No files collected from source: $SOURCE" >&2
  exit 1
fi

log "Found ${#FILES[@]} files to upload."

# upload sequentially (for simplicity). Could be parallelized if needed.
overall_ok=0
for f in "${FILES[@]}"; do
  log "Processing: $f"
  # call transfer_file with proper target type & spec
  if [[ "$TTYPE" == "http" ]]; then
    transfer_file "$f" "http" "$TSPEC" || overall_ok=1
  elif [[ "$TTYPE" == "ssh" ]]; then
    transfer_file "$f" "ssh" "$TSPEC" || overall_ok=1
  elif [[ "$TTYPE" == "s3" ]]; then
    transfer_file "$f" "s3" "$TSPEC" || overall_ok=1
  fi
done

if [[ $overall_ok -eq 0 ]]; then
  log "All transfers finished OK."
  exit 0
else
  log "Some transfers failed. See audit log: $AUDIT_LOG and status files in $STATUS_DIR"
  exit 1
fi
