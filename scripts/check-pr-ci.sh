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

# Get all check runs (use text output as --json may be empty)
CHECKS_TEXT=$(gh pr checks "$PR_NUMBER" 2>/dev/null)

if [ -z "$CHECKS_TEXT" ]; then
    echo -e "${YELLOW}No checks found yet. CI may still be starting...${NC}"
else
    # Count statuses from text output
    TOTAL=$(echo "$CHECKS_TEXT" | wc -l | tr -d ' \n')
    PASSED=$(echo "$CHECKS_TEXT" | grep -c "	pass	" || true)
    FAILED=$(echo "$CHECKS_TEXT" | grep -c "	fail	" || true)
    PENDING=$(echo "$CHECKS_TEXT" | grep -cE "	(pending|queued|in_progress)	" || true)
    SKIPPED=$(echo "$CHECKS_TEXT" | grep -c "	skipped	" || true)

    echo -e "Total: ${TOTAL} | ${GREEN}Passed: ${PASSED}${NC} | ${RED}Failed: ${FAILED}${NC} | ${YELLOW}Pending: ${PENDING}${NC} | Skipped: ${SKIPPED}"
    echo ""

    # Show each check (format: name<tab>status<tab>time<tab>url)
    echo "$CHECKS_TEXT" | while IFS=$'\t' read -r name status time url; do
        if [ "$status" = "pass" ]; then
            echo -e "  ${GREEN}✓${NC} $name ($time)"
        elif [ "$status" = "fail" ]; then
            echo -e "  ${RED}✗${NC} $name ($time)"
        elif [ "$status" = "pending" ] || [ "$status" = "queued" ]; then
            echo -e "  ${YELLOW}○${NC} $name (pending)"
        elif [ "$status" = "in_progress" ]; then
            echo -e "  ${YELLOW}◐${NC} $name (running)"
        elif [ "$status" = "skipped" ]; then
            echo -e "  ${BLUE}—${NC} $name (skipped)"
        else
            echo -e "  ? $name ($status)"
        fi
    done
fi

echo ""

# Show failed checks details (extract from text output)
FAILED_CHECKS=$(echo "$CHECKS_TEXT" | grep "fail" | awk '{print $1}')

if [ -n "$FAILED_CHECKS" ]; then
    echo -e "${RED}=== Failed Checks Details ===${NC}"
    echo ""

    # Get the run ID for failed workflows
    RUNS=$(gh run list --branch "$PR_BRANCH" --limit 10 --json databaseId,name,status,conclusion,headSha 2>/dev/null)

    echo "$FAILED_CHECKS" | while read -r check_name; do
        echo -e "${RED}--- $check_name ---${NC}"

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
        echo ""
    done
fi

# Show pending checks (extract from text output)
PENDING_CHECKS=$(echo "$CHECKS_TEXT" | grep -E "pending|queued|in_progress" | awk '{print $1}')

if [ -n "$PENDING_CHECKS" ]; then
    echo -e "${YELLOW}=== Pending Checks ===${NC}"
    echo "$PENDING_CHECKS" | while read -r check_name; do
        echo "  - $check_name"
    done
    echo ""
    echo -e "${YELLOW}Run this script again to check progress.${NC}"
fi

# Show PR reviews
echo -e "${BLUE}=== Reviews ===${NC}"
echo ""

REVIEWS=$(gh api "repos/{owner}/{repo}/pulls/$PR_NUMBER/reviews" --jq '.[] | select(.state != "PENDING") | {author: .user.login, state: .state, body: .body}' 2>/dev/null)

if [ -z "$REVIEWS" ]; then
    echo "No reviews yet."
else
    gh api "repos/{owner}/{repo}/pulls/$PR_NUMBER/reviews" --jq '.[] | select(.state != "PENDING") | "\(.state)\t\(.user.login)"' 2>/dev/null | while IFS=$'\t' read -r state author; do
        if [ "$state" = "APPROVED" ]; then
            echo -e "  ${GREEN}✓${NC} $author (approved)"
        elif [ "$state" = "CHANGES_REQUESTED" ]; then
            echo -e "  ${RED}✗${NC} $author (changes requested)"
        elif [ "$state" = "COMMENTED" ]; then
            echo -e "  ${YELLOW}○${NC} $author (commented)"
        else
            echo -e "  ? $author ($state)"
        fi
    done
fi
echo ""

# Show review comments count
REVIEW_COMMENTS=$(gh api "repos/{owner}/{repo}/pulls/$PR_NUMBER/comments" 2>/dev/null)
COMMENT_COUNT=$(echo "$REVIEW_COMMENTS" | jq 'length')

if [ "$COMMENT_COUNT" -gt 0 ]; then
    echo -e "${YELLOW}=== Review Comments ($COMMENT_COUNT) ===${NC}"
    echo ""

    echo "$REVIEW_COMMENTS" | jq -r '.[] | "[\(.path | split("/") | last):\(.line // .original_line // "?")] \(.body | split("\n")[0] | if length > 80 then .[0:77] + "..." else . end)"' | head -20

    if [ "$COMMENT_COUNT" -gt 20 ]; then
        echo ""
        echo "... and $((COMMENT_COUNT - 20)) more comments"
    fi
    echo ""
fi

# Summary
echo ""
FAILED=${FAILED:-0}
PENDING=${PENDING:-0}
if [ "$FAILED" -gt 0 ] 2>/dev/null; then
    echo -e "${RED}CI Status: FAILED${NC}"
    exit 1
elif [ "$PENDING" -gt 0 ] 2>/dev/null; then
    echo -e "${YELLOW}CI Status: IN PROGRESS${NC}"
    exit 0
else
    echo -e "${GREEN}CI Status: PASSED${NC}"
    exit 0
fi
