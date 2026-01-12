#!/bin/bash
# Check PR CI status and show failure details
# Usage: ./scripts/check-pr-ci.sh [PR_NUMBER]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get PR number from argument or find the current branch's PR
PR_NUMBER=${1:-$(gh pr view --json number -q '.number' 2>/dev/null || echo "")}

if [ -z "$PR_NUMBER" ]; then
    echo -e "${RED}Error: No PR number provided and no PR found for current branch${NC}"
    echo "Usage: $0 [PR_NUMBER]"
    exit 1
fi

echo -e "${BLUE}=== PR #${PR_NUMBER} CI Status ===${NC}"
echo ""

# Get PR info
PR_INFO=$(gh pr view "$PR_NUMBER" --json title,state,url,headRefName)
PR_TITLE=$(echo "$PR_INFO" | jq -r '.title')
PR_STATE=$(echo "$PR_INFO" | jq -r '.state')
PR_URL=$(echo "$PR_INFO" | jq -r '.url')
PR_BRANCH=$(echo "$PR_INFO" | jq -r '.headRefName')

echo -e "Title:  ${PR_TITLE}"
echo -e "Branch: ${PR_BRANCH}"
echo -e "State:  ${PR_STATE}"
echo -e "URL:    ${PR_URL}"
echo ""

# Get check runs status
echo -e "${BLUE}=== Check Runs ===${NC}"
echo ""

# Get all check runs
CHECKS=$(gh pr checks "$PR_NUMBER" --json name,state,conclusion,startedAt,completedAt,detailsUrl 2>/dev/null || echo "[]")

if [ "$CHECKS" = "[]" ] || [ -z "$CHECKS" ]; then
    echo -e "${YELLOW}No checks found yet. CI may still be starting...${NC}"
    exit 0
fi

# Count statuses
TOTAL=$(echo "$CHECKS" | jq 'length')
PASSED=$(echo "$CHECKS" | jq '[.[] | select(.conclusion == "success")] | length')
FAILED=$(echo "$CHECKS" | jq '[.[] | select(.conclusion == "failure")] | length')
PENDING=$(echo "$CHECKS" | jq '[.[] | select(.state == "pending" or .state == "queued" or .state == "in_progress")] | length')
SKIPPED=$(echo "$CHECKS" | jq '[.[] | select(.conclusion == "skipped")] | length')

echo -e "Total: ${TOTAL} | ${GREEN}Passed: ${PASSED}${NC} | ${RED}Failed: ${FAILED}${NC} | ${YELLOW}Pending: ${PENDING}${NC} | Skipped: ${SKIPPED}"
echo ""

# Show each check
echo "$CHECKS" | jq -r '.[] | [.name, .state, .conclusion // "—"] | @tsv' | while IFS=$'\t' read -r name state conclusion; do
    if [ "$conclusion" = "success" ]; then
        echo -e "  ${GREEN}✓${NC} $name"
    elif [ "$conclusion" = "failure" ]; then
        echo -e "  ${RED}✗${NC} $name"
    elif [ "$state" = "pending" ] || [ "$state" = "queued" ]; then
        echo -e "  ${YELLOW}○${NC} $name (pending)"
    elif [ "$state" = "in_progress" ]; then
        echo -e "  ${YELLOW}◐${NC} $name (running)"
    elif [ "$conclusion" = "skipped" ]; then
        echo -e "  ${BLUE}—${NC} $name (skipped)"
    else
        echo -e "  ? $name ($state/$conclusion)"
    fi
done

echo ""

# Show failed checks details
FAILED_CHECKS=$(echo "$CHECKS" | jq -r '.[] | select(.conclusion == "failure") | .name')

if [ -n "$FAILED_CHECKS" ]; then
    echo -e "${RED}=== Failed Checks Details ===${NC}"
    echo ""

    # Get the run ID for failed workflows
    RUNS=$(gh run list --branch "$PR_BRANCH" --limit 10 --json databaseId,name,status,conclusion,headSha 2>/dev/null)

    echo "$FAILED_CHECKS" | while read -r check_name; do
        echo -e "${RED}--- $check_name ---${NC}"

        # Find the run ID for this check
        RUN_ID=$(echo "$RUNS" | jq -r --arg name "$check_name" '.[] | select(.name == $name and .conclusion == "failure") | .databaseId' | head -1)

        if [ -n "$RUN_ID" ] && [ "$RUN_ID" != "null" ]; then
            echo "Run ID: $RUN_ID"
            echo ""

            # Get failed jobs
            FAILED_JOBS=$(gh run view "$RUN_ID" --json jobs -q '.jobs[] | select(.conclusion == "failure") | .name' 2>/dev/null)

            if [ -n "$FAILED_JOBS" ]; then
                echo "$FAILED_JOBS" | while read -r job_name; do
                    echo -e "${YELLOW}Job: $job_name${NC}"
                    echo ""

                    # Get job logs (last 50 lines of failed step)
                    echo "Fetching logs..."
                    gh run view "$RUN_ID" --log-failed 2>/dev/null | tail -100 || echo "Could not fetch logs"
                    echo ""
                done
            fi
        else
            # Try to find by workflow name pattern
            for workflow in "CI" "E2E Tests"; do
                RUN_ID=$(echo "$RUNS" | jq -r --arg name "$workflow" '.[] | select(.name == $name and .conclusion == "failure") | .databaseId' | head -1)
                if [ -n "$RUN_ID" ] && [ "$RUN_ID" != "null" ]; then
                    echo "Found in workflow: $workflow (Run ID: $RUN_ID)"
                    echo ""
                    gh run view "$RUN_ID" --log-failed 2>/dev/null | tail -100 || echo "Could not fetch logs"
                    break
                fi
            done
        fi
        echo ""
    done
fi

# Show pending checks
PENDING_CHECKS=$(echo "$CHECKS" | jq -r '.[] | select(.state == "pending" or .state == "queued" or .state == "in_progress") | .name')

if [ -n "$PENDING_CHECKS" ]; then
    echo -e "${YELLOW}=== Pending Checks ===${NC}"
    echo "$PENDING_CHECKS" | while read -r check_name; do
        echo "  - $check_name"
    done
    echo ""
    echo -e "${YELLOW}Run this script again to check progress.${NC}"
fi

# Summary
echo ""
if [ "$FAILED" -gt 0 ]; then
    echo -e "${RED}CI Status: FAILED${NC}"
    exit 1
elif [ "$PENDING" -gt 0 ]; then
    echo -e "${YELLOW}CI Status: IN PROGRESS${NC}"
    exit 0
else
    echo -e "${GREEN}CI Status: PASSED${NC}"
    exit 0
fi
