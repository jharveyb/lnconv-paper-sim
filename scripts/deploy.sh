#!/usr/bin/env bash
# Drive remote lnconv-sim runs over SSH by shipping a prebuilt binary.
#
# Prerequisite: `cd lnconv-sim && cargo build --release` on the laptop. The
# remote box only needs glibc (compatible with the laptop's) + tmux + rsync
# -- no Rust toolchain, no apt packages beyond the basics.
#
# Subcommands:
#   bootstrap <user@host>                  install minimal deps + mkdir + ship binary,
#                                          init_data, configs (once per box)
#   run <user@host> <config.toml> ...      re-ship binary + configs, then start a
#                                          sequential queue inside a detached tmux
#                                          session
#   status <user@host>                     show running queues, lnconv processes,
#                                          and tails of the latest sim + sysmon logs
#
# Env overrides:
#   LNCONV_REMOTE_DIR        lnconv-sim   (path relative to remote $HOME)
#   LNCONV_BINARY            lnconv-sim/target/release/lnconv  (local path)
#   LNCONV_SYSMON_INTERVAL   30           (seconds between system samples)
#
# SSH heredocs use quoted <<'EOF' and pass the remote-dir path (and queue
# tag) as positional args to `bash -s`, so no local expansion happens
# inside the heredoc body. The one exception is the `cat >` pipe in
# cmd_run, which intentionally local-expands $REMOTE_DIR_PATH and ${qts}
# in the SSH command-line string (its stdin is the queue script content).
set -euo pipefail

REMOTE_DIR_PATH="${LNCONV_REMOTE_DIR:-lnconv-sim}"
LOCAL_BINARY="${LNCONV_BINARY:-lnconv-sim/target/release/lnconv}"
SYSMON_INTERVAL="${LNCONV_SYSMON_INTERVAL:-120}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

usage() {
  cat <<EOF
Usage:
  $0 bootstrap <user@host>
  $0 run       <user@host> <config.toml> [<config.toml> ...]
  $0 status    <user@host>

Build the binary locally first:
  cd lnconv-sim && cargo build --release

Env overrides:
  LNCONV_REMOTE_DIR=\$HOME/$REMOTE_DIR_PATH
  LNCONV_BINARY=$LOCAL_BINARY
  LNCONV_SYSMON_INTERVAL=${SYSMON_INTERVAL}s

If the binary fails on the remote with "GLIBC_2.XX not found", the remote's
glibc is older than the laptop's. Build a static binary instead:
  rustup target add x86_64-unknown-linux-musl
  cd lnconv-sim && cargo build --release --target x86_64-unknown-linux-musl
  LNCONV_BINARY=lnconv-sim/target/x86_64-unknown-linux-musl/release/lnconv \\
    $0 bootstrap user@host
EOF
}

require_binary() {
  cd "$REPO_ROOT"
  [ -x "$LOCAL_BINARY" ] || {
    echo "binary not found or not executable: $LOCAL_BINARY" >&2
    echo "build it first: (cd lnconv-sim && cargo build --release)" >&2
    exit 1
  }
}

ship_payload() {
  # rsync binary, init_data, configs, sysmon.py -> remote. Idempotent + incremental.
  local host="$1"
  rsync -ah "$LOCAL_BINARY" "$host:$REMOTE_DIR_PATH/lnconv"
  rsync -ah lnconv-sim/init_data/ "$host:$REMOTE_DIR_PATH/init_data/"
  rsync -ahq --include='*.toml' --include='*/' --exclude='*' \
    lnconv-sim/configs/ "$host:$REMOTE_DIR_PATH/configs/"
  rsync -ah scripts/sysmon.py "$host:$REMOTE_DIR_PATH/sysmon.py"
}

is_local() {
  case "$1" in localhost|local) return 0 ;; *) return 1 ;; esac
}

resolve_config() {
  # Accept a config arg in any of these forms and emit the canonical
  # lnconv-sim/configs/<name>.toml path on stdout:
  #   foo                          -> lnconv-sim/configs/foo.toml
  #   foo.toml                     -> lnconv-sim/configs/foo.toml
  #   configs/foo.toml             -> lnconv-sim/configs/foo.toml
  #   lnconv-sim/configs/foo.toml  -> lnconv-sim/configs/foo.toml
  local cfg="$1"
  [[ "$cfg" == *.toml ]] || cfg="${cfg}.toml"
  for try in "lnconv-sim/configs/$(basename "$cfg")" "$cfg"; do
    if [ -f "$try" ]; then echo "$try"; return 0; fi
  done
  return 1
}

cmd_bootstrap() {
  local host="$1"
  if is_local "$host"; then
    cat <<EOF
localhost doesn't need bootstrap. Just build the binary:
  (cd lnconv-sim && cargo build --release)
Then start a queue:
  $0 run localhost <config> [<config> ...]
EOF
    return
  fi
  require_binary
  echo ">> bootstrapping $host"

  # apt step uses `ssh -t` so sudo can prompt on the tty if the remote
  # account isn't NOPASSWD. Both apt commands run under one sudo invocation
  # (single password prompt). To skip the prompt entirely, configure
  # passwordless sudo on the remote for these commands.
  echo "-- apt deps (sudo may prompt for password)"
  # ssh -t "$host" "sudo sh -c 'apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tmux rsync ca-certificates python3 python3-psutil'"

  echo "-- creating remote dirs"
  ssh "$host" bash -s "$REMOTE_DIR_PATH" <<'EOF'
set -euo pipefail
mkdir -p "$HOME/$1/logs" "$HOME/$1/sim_output"
EOF

  echo ">> shipping binary + init_data + configs"
  ship_payload "$host"

  echo ">> smoke check"
  ssh "$host" bash -s "$REMOTE_DIR_PATH" <<'EOF'
set -euo pipefail
cd "$HOME/$1"
chmod +x lnconv
./lnconv --help | head -1
echo "-- ready"
EOF
}

cmd_run() {
  local host="$1"; shift
  local configs=("$@")
  [ ${#configs[@]} -eq 0 ] && { usage; exit 1; }
  require_binary

  local resolved=()
  for cfg in "${configs[@]}"; do
    local r
    if ! r=$(resolve_config "$cfg"); then
      echo "config not found: $cfg (looked in lnconv-sim/configs/)" >&2
      exit 1
    fi
    resolved+=("$r")
  done
  configs=("${resolved[@]}")

  local qts; qts=$(date -u +%Y%m%dT%H%M%SZ)

  # Local vs remote: choose binary + sysmon paths used inside the queue
  # script. Local mode skips the rsync step entirely.
  local binary_path sysmon_path
  if is_local "$host"; then
    binary_path="./target/release/lnconv"
    sysmon_path="$REPO_ROOT/scripts/sysmon.py"
  else
    echo ">> shipping binary + configs"
    ship_payload "$host"
    binary_path="./lnconv"
    sysmon_path="./sysmon.py"
  fi

  # Build the per-config queue body. Each line is fully expanded locally;
  # no $ remains, so it drops verbatim into the outer heredoc below.
  local queue_body=""
  for cfg in "${configs[@]}"; do
    # Configs resolve to lnconv-sim/configs/foo.toml. The queue script's
    # CWD is lnconv-sim/ (local) or its flattened equivalent (remote), so
    # we strip the lnconv-sim/ prefix.
    local rel="${cfg#lnconv-sim/}"
    local base; base=$(basename "$cfg" .toml)
    local label="${qts}-${base}"
    queue_body+="
echo '=== ${label} ==='
${binary_path} -c '${rel}' --name '${label}' 2>&1 | tee 'logs/${label}.log'"
  done

  local queue_script
  queue_script=$(cat <<EOF
#!/usr/bin/env bash
set -e
mkdir -p logs sim_output

# Background system + lnconv resource monitor (see sysmon.py).
python3 ${sysmon_path} --interval ${SYSMON_INTERVAL} > "logs/sysmon-${qts}.log" 2>&1 &
SYSMON_PID=\$!
trap "kill \$SYSMON_PID 2>/dev/null || true" EXIT

# queue
${queue_body}

echo "=== queue complete: ${qts} ==="
date -u +%Y-%m-%dT%H:%M:%SZ > "logs/queue-${qts}.done"
EOF
)

  if is_local "$host"; then
    echo ">> writing queue script + launching local tmux session lnconv-${qts}"
    cd "$REPO_ROOT/lnconv-sim"
    printf '%s\n' "$queue_script" > ".queue-${qts}.sh"
    chmod +x ".queue-${qts}.sh"
    tmux new-session -d -s "lnconv-${qts}" "./.queue-${qts}.sh"

    cat <<EOF

Queue started locally: lnconv-${qts} (${#configs[@]} config(s))

  Attach:  tmux attach -t lnconv-${qts}
  Tail:    tail -F lnconv-sim/logs/${qts}-*.log
  Sysmon:  tail -F lnconv-sim/logs/sysmon-${qts}.log
  Status:  $0 status localhost
  Outputs: lnconv-sim/sim_output/${qts}-*.parquet
EOF
  else
    echo ">> writing queue script + launching tmux session lnconv-${qts}"
    # shellcheck disable=SC2029  # $REMOTE_DIR_PATH and ${qts} are intentionally local-expanded
    printf '%s\n' "$queue_script" \
      | ssh "$host" "cat > \"\$HOME/$REMOTE_DIR_PATH/.queue-${qts}.sh\" && chmod +x \"\$HOME/$REMOTE_DIR_PATH/.queue-${qts}.sh\""

    ssh "$host" bash -s "$REMOTE_DIR_PATH" "$qts" <<'EOF'
set -euo pipefail
cd "$HOME/$1"
tmux new-session -d -s "lnconv-$2" "./.queue-$2.sh"
EOF

    cat <<EOF

Queue started: lnconv-${qts} (${#configs[@]} config(s))

  Attach:  ssh -t $host 'tmux attach -t lnconv-${qts}'
  Tail:    ssh $host 'tail -F $REMOTE_DIR_PATH/logs/${qts}-*.log'
  Sysmon:  ssh $host 'tail -F $REMOTE_DIR_PATH/logs/sysmon-${qts}.log'
  Status:  $0 status $host
  Fetch:   rsync -avh '$host:$REMOTE_DIR_PATH/sim_output/${qts}-*' ./sim_output/
EOF
  fi
}

# Status logic that runs in the CWD of an lnconv-sim install (the dir
# holding logs/, sim_output/, configs/). Defined as a function so it can be
# invoked directly on localhost OR shipped over SSH via `declare -f`.
_status_body() {
  echo "== running tmux sessions =="
  tmux ls 2>/dev/null | grep '^lnconv-' || echo "(none)"
  echo

  echo "== lnconv processes =="
  local pids; pids=$(pgrep -x lnconv 2>/dev/null || true)
  if [ -z "$pids" ]; then
    echo "(none running)"
  else
    for p in $pids; do
      local rss cmd
      rss=$(awk '/^VmRSS:/{print $2 " " $3}' /proc/"$p"/status 2>/dev/null)
      cmd=$(tr '\0' ' ' < /proc/"$p"/cmdline 2>/dev/null)
      echo "pid=$p rss=$rss cmd=$cmd"
    done
  fi
  echo

  echo "== completed queues =="
  local done_files; done_files=$(ls -1 logs/queue-*.done 2>/dev/null || true)
  if [ -z "$done_files" ]; then
    echo "(none)"
  else
    echo "$done_files" | sed 's|logs/queue-||; s|\.done$||'
  fi
  echo

  # Newest non-sysmon log
  local last=""
  for f in logs/*.log; do
    case "$f" in logs/sysmon-*.log) continue ;; esac
    [ -f "$f" ] || continue
    if [ -z "$last" ] || [ "$f" -nt "$last" ]; then last="$f"; fi
  done
  if [ -n "$last" ]; then
    echo "== latest sim log: $(basename "$last") =="
    tail -5 "$last"
    echo
  fi

  # Newest sysmon log
  last=""
  for f in logs/sysmon-*.log; do
    [ -f "$f" ] || continue
    if [ -z "$last" ] || [ "$f" -nt "$last" ]; then last="$f"; fi
  done
  if [ -n "$last" ]; then
    echo "== latest sysmon: $(basename "$last") =="
    tail -3 "$last"
  fi
}

cmd_status() {
  local host="$1"
  if is_local "$host"; then
    ( cd "$REPO_ROOT/lnconv-sim" && _status_body )
  else
    # Ship the function source + a one-liner that cd's to the install dir
    # and calls it. Quoted heredoc on the local end; printf prevents the
    # remote $HOME from being expanded locally.
    {
      declare -f _status_body
      # shellcheck disable=SC2016  # $HOME is intentionally literal -- the remote shell expands it
      printf 'set -u\ncd "$HOME/%s" 2>/dev/null || { echo "no install at $HOME/%s"; exit 1; }\n_status_body\n' \
        "$REMOTE_DIR_PATH" "$REMOTE_DIR_PATH"
    } | ssh "$host" bash -s
  fi
}

sub="${1:-}"
[ $# -ge 1 ] && shift
case "$sub" in
  bootstrap) [ $# -ge 1 ] || { usage; exit 1; }; cmd_bootstrap "$@" ;;
  run)       [ $# -ge 2 ] || { usage; exit 1; }; cmd_run "$@" ;;
  status)    [ $# -ge 1 ] || { usage; exit 1; }; cmd_status "$@" ;;
  -h|--help) usage ;;
  *)         usage; exit 1 ;;
esac
