#!/bin/bash
# verify-sync.sh - Verify session sync between two hosts
#
# Compares .claude/projects session files between ARM and x86 hosts.
# Sessions should be identical or one should be a prefix of the other
# (same entries, just one has more recent messages appended).

set -euo pipefail

ARM_HOST="ubuntu@184.72.40.255"
X86_HOST="ubuntu@50.18.109.164"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/fcvm-ec2}"
WORK_DIR="/tmp/verify-sync-$$"

mkdir -p "$WORK_DIR"
trap "rm -rf $WORK_DIR" EXIT

echo "=== Claude Code Session Sync Verification ==="
echo ""

# Function to extract session manifest from a host
extract_manifest() {
    local host=$1
    local output=$2

    ssh -i "$SSH_KEY" "$host" 'find ~/.claude/projects -name "*.jsonl" -type f 2>/dev/null | while read f; do
        rel_path="${f#$HOME/.claude/projects/}"
        # Extract UUIDs in order (uuid field from each line)
        uuids=$(cat "$f" 2>/dev/null | jq -r ".uuid // empty" 2>/dev/null | tr "\n" ",")
        entry_count=$(wc -l < "$f" 2>/dev/null | tr -d " ")
        echo "$rel_path|$entry_count|$uuids"
    done' > "$output"
}

echo "Fetching session manifest from ARM..."
extract_manifest "$ARM_HOST" "$WORK_DIR/arm_manifest.txt"
arm_count=$(wc -l < "$WORK_DIR/arm_manifest.txt" | tr -d ' ')
echo "  Found $arm_count sessions on ARM"

echo "Fetching session manifest from x86..."
extract_manifest "$X86_HOST" "$WORK_DIR/x86_manifest.txt"
x86_count=$(wc -l < "$WORK_DIR/x86_manifest.txt" | tr -d ' ')
echo "  Found $x86_count sessions on x86"

echo ""
echo "=== Comparing Sessions ==="

# Build lookup maps
declare -A arm_sessions
declare -A x86_sessions

while IFS='|' read -r path count uuids; do
    arm_sessions["$path"]="$count|$uuids"
done < "$WORK_DIR/arm_manifest.txt"

while IFS='|' read -r path count uuids; do
    x86_sessions["$path"]="$count|$uuids"
done < "$WORK_DIR/x86_manifest.txt"

# Find all unique paths
all_paths=$(cat "$WORK_DIR/arm_manifest.txt" "$WORK_DIR/x86_manifest.txt" | cut -d'|' -f1 | sort -u)

identical=0
arm_ahead=0
x86_ahead=0
diverged=0
arm_only=0
x86_only=0
diverged_list=""

for path in $all_paths; do
    arm_data="${arm_sessions[$path]:-}"
    x86_data="${x86_sessions[$path]:-}"

    if [[ -z "$arm_data" ]]; then
        ((x86_only++))
        continue
    fi

    if [[ -z "$x86_data" ]]; then
        ((arm_only++))
        continue
    fi

    # Both exist - compare
    arm_count="${arm_data%%|*}"
    x86_count="${x86_data%%|*}"
    arm_uuids="${arm_data#*|}"
    x86_uuids="${x86_data#*|}"

    if [[ "$arm_uuids" == "$x86_uuids" ]]; then
        ((identical++))
    elif [[ "$x86_uuids" == "$arm_uuids"* ]]; then
        # x86 starts with ARM's UUIDs - x86 is ahead
        ((x86_ahead++))
    elif [[ "$arm_uuids" == "$x86_uuids"* ]]; then
        # ARM starts with x86's UUIDs - ARM is ahead
        ((arm_ahead++))
    else
        # Diverged - UUIDs don't match prefix
        ((diverged++))
        session_name=$(basename "$path")
        diverged_list="$diverged_list\n  $session_name (ARM:$arm_count x86:$x86_count)"
    fi
done

echo ""
echo "Results:"
echo "  ✓ Identical:     $identical"
echo "  → ARM ahead:     $arm_ahead"
echo "  ← x86 ahead:     $x86_ahead"
echo "  ✗ Diverged:      $diverged"
echo "  ◦ ARM only:      $arm_only"
echo "  ◦ x86 only:      $x86_only"
echo ""

total=$((identical + arm_ahead + x86_ahead + diverged))
if [[ $diverged -eq 0 ]]; then
    echo "✅ All $total shared sessions are in sync (one is prefix of other)"
else
    echo "⚠️  $diverged sessions have diverged histories!"
    echo -e "$diverged_list"
fi

# Show some diverged examples for debugging
if [[ $diverged -gt 0 ]]; then
    echo ""
    echo "=== Diverged Session Details (first 3) ==="

    count=0
    for path in $all_paths; do
        [[ $count -ge 3 ]] && break

        arm_data="${arm_sessions[$path]:-}"
        x86_data="${x86_sessions[$path]:-}"

        [[ -z "$arm_data" || -z "$x86_data" ]] && continue

        arm_uuids="${arm_data#*|}"
        x86_uuids="${x86_data#*|}"

        # Check if diverged
        if [[ "$arm_uuids" != "$x86_uuids" ]] && \
           [[ "$x86_uuids" != "$arm_uuids"* ]] && \
           [[ "$arm_uuids" != "$x86_uuids"* ]]; then
            echo ""
            echo "Session: $path"
            echo "  ARM UUIDs (first 5): $(echo "$arm_uuids" | tr ',' '\n' | head -5 | tr '\n' ' ')"
            echo "  x86 UUIDs (first 5): $(echo "$x86_uuids" | tr ',' '\n' | head -5 | tr '\n' ' ')"
            ((count++))
        fi
    done
fi
