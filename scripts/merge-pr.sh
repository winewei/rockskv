#!/bin/bash
# Safe PR merge script - checks all conditions before merging
# Usage: ./scripts/merge-pr.sh [PR_NUMBER] [--force]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Parse arguments
PR_NUMBER=""
FORCE=false

for arg in "$@"; do
    case $arg in
        --force|-f)
            FORCE=true
            ;;
        *)
            if [ -z "$PR_NUMBER" ]; then
                PR_NUMBER="$arg"
            fi
            ;;
    esac
done

# Get PR number if not provided
if [ -z "$PR_NUMBER" ]; then
    PR_NUMBER=$(gh pr view --json number -q '.number' 2>/dev/null || echo "")
fi

if [ -z "$PR_NUMBER" ]; then
    echo -e "${RED}Error: No PR found for current branch${NC}"
    echo "Usage: $0 [PR_NUMBER] [--force]"
    exit 1
fi

echo -e "${BLUE}=== PR #${PR_NUMBER} Merge Tool ===${NC}"
echo ""

# Run merge readiness check
echo "Running pre-merge checks..."
echo ""

if ! "$SCRIPT_DIR/check-pr-mergeable.sh" "$PR_NUMBER"; then
    if [ "$FORCE" = true ]; then
        echo ""
        echo -e "${YELLOW}Warning: Forcing merge despite errors${NC}"
    else
        echo ""
        echo -e "${RED}Merge blocked. Use --force to override (not recommended).${NC}"
        exit 1
    fi
fi

echo ""

# Get PR info for merge message
PR_INFO=$(gh pr view "$PR_NUMBER" --json title,body,headRefName)
PR_TITLE=$(echo "$PR_INFO" | jq -r '.title')
PR_BODY=$(echo "$PR_INFO" | jq -r '.body // ""')

# Confirm merge
echo -e "${YELLOW}Ready to merge:${NC}"
echo "  Title: $PR_TITLE"
echo "  PR: #$PR_NUMBER"
echo ""

read -p "Proceed with squash merge? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Merge cancelled."
    exit 0
fi

# Perform merge
echo ""
echo "Merging PR #$PR_NUMBER..."

gh pr merge "$PR_NUMBER" --squash --delete-branch

echo ""
echo -e "${GREEN}PR #${PR_NUMBER} merged successfully!${NC}"
