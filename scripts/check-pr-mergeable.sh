#!/bin/bash
# Check if PR is ready to merge
# Usage: ./scripts/check-pr-mergeable.sh [PR_NUMBER]
# Exit codes: 0 = ready to merge, 1 = not ready, 2 = no PR found

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Get PR number
PR_NUMBER=${1:-$(gh pr view --json number -q '.number' 2>/dev/null || echo "")}

if [ -z "$PR_NUMBER" ]; then
    echo -e "${YELLOW}No PR found for current branch${NC}"
    exit 2
fi

echo -e "${BLUE}=== Checking PR #${PR_NUMBER} Merge Readiness ===${NC}"
echo ""

ERRORS=0
WARNINGS=0

# 1. Check CI status
echo -e "${BLUE}[1/4] CI Status${NC}"
CHECKS_TEXT=$(gh pr checks "$PR_NUMBER" 2>/dev/null || echo "")

if [ -z "$CHECKS_TEXT" ]; then
    echo -e "  ${YELLOW}○${NC} No CI checks found (may still be starting)"
    WARNINGS=$((WARNINGS + 1))
else
    FAILED=$(echo "$CHECKS_TEXT" | grep -c "	fail	" || true)
    PENDING=$(echo "$CHECKS_TEXT" | grep -cE "	(pending|queued|in_progress)	" || true)
    PASSED=$(echo "$CHECKS_TEXT" | grep -c "	pass	" || true)

    if [ "$FAILED" -gt 0 ]; then
        echo -e "  ${RED}✗${NC} $FAILED CI check(s) failed"
        ERRORS=$((ERRORS + 1))
    elif [ "$PENDING" -gt 0 ]; then
        echo -e "  ${YELLOW}○${NC} $PENDING CI check(s) still running"
        WARNINGS=$((WARNINGS + 1))
    else
        echo -e "  ${GREEN}✓${NC} All $PASSED CI checks passed"
    fi
fi

# 2. Check review status
echo -e "${BLUE}[2/4] Review Status${NC}"
REVIEWS=$(gh api "repos/{owner}/{repo}/pulls/$PR_NUMBER/reviews" 2>/dev/null || echo "[]")

CHANGES_REQUESTED=$(echo "$REVIEWS" | jq '[.[] | select(.state == "CHANGES_REQUESTED")] | length')
APPROVED=$(echo "$REVIEWS" | jq '[.[] | select(.state == "APPROVED")] | length')

if [ "$CHANGES_REQUESTED" -gt 0 ]; then
    echo -e "  ${RED}✗${NC} $CHANGES_REQUESTED review(s) requested changes"
    ERRORS=$((ERRORS + 1))
elif [ "$APPROVED" -gt 0 ]; then
    echo -e "  ${GREEN}✓${NC} $APPROVED approval(s)"
else
    echo -e "  ${YELLOW}○${NC} No approvals yet (only comments)"
    # Not an error, just a warning
fi

# 3. Check unresolved review comments
echo -e "${BLUE}[3/4] Review Comments${NC}"
COMMENTS=$(gh api "repos/{owner}/{repo}/pulls/$PR_NUMBER/comments" 2>/dev/null || echo "[]")
COMMENT_COUNT=$(echo "$COMMENTS" | jq 'length')

# Check for unresolved threads (simplified - just count comments)
if [ "$COMMENT_COUNT" -gt 0 ]; then
    # Get unique files with comments
    FILES_WITH_COMMENTS=$(echo "$COMMENTS" | jq -r '.[].path' | sort -u | wc -l | tr -d ' ')
    echo -e "  ${YELLOW}○${NC} $COMMENT_COUNT comment(s) on $FILES_WITH_COMMENTS file(s)"
    echo "     Consider reviewing and resolving comments before merge"
else
    echo -e "  ${GREEN}✓${NC} No review comments"
fi

# 4. Check merge conflicts
echo -e "${BLUE}[4/4] Merge Conflicts${NC}"
MERGEABLE=$(gh pr view "$PR_NUMBER" --json mergeable,mergeStateStatus -q '.mergeable + ":" + .mergeStateStatus' 2>/dev/null || echo "UNKNOWN:UNKNOWN")
MERGEABLE_STATE=$(echo "$MERGEABLE" | cut -d: -f1)
MERGE_STATUS=$(echo "$MERGEABLE" | cut -d: -f2)

if [ "$MERGEABLE_STATE" = "MERGEABLE" ]; then
    echo -e "  ${GREEN}✓${NC} No merge conflicts"
elif [ "$MERGEABLE_STATE" = "CONFLICTING" ]; then
    echo -e "  ${RED}✗${NC} Has merge conflicts - needs rebase"
    ERRORS=$((ERRORS + 1))
elif [ "$MERGEABLE_STATE" = "UNKNOWN" ]; then
    echo -e "  ${YELLOW}○${NC} Merge status unknown (checking...)"
    WARNINGS=$((WARNINGS + 1))
else
    echo -e "  ${YELLOW}○${NC} Merge state: $MERGE_STATUS"
fi

# Summary
echo ""
echo -e "${BLUE}=== Summary ===${NC}"

if [ "$ERRORS" -gt 0 ]; then
    echo -e "${RED}NOT READY TO MERGE${NC}"
    echo -e "  Errors: $ERRORS"
    [ "$WARNINGS" -gt 0 ] && echo -e "  Warnings: $WARNINGS"
    echo ""
    echo "Fix the errors above before merging."
    exit 1
elif [ "$WARNINGS" -gt 0 ]; then
    echo -e "${YELLOW}MERGE WITH CAUTION${NC}"
    echo -e "  Warnings: $WARNINGS"
    echo ""
    echo "No blocking errors, but review warnings above."
    exit 0
else
    echo -e "${GREEN}READY TO MERGE${NC}"
    echo ""
    echo "All checks passed. Safe to merge."
    exit 0
fi
