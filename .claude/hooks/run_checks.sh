#!/usr/bin/env bash
set -euo pipefail

cd "${CLAUDE_PROJECT_DIR:-.}"

# 只有当工作区/暂存区有变化时才跑，避免每次 Stop 都跑一遍
if git diff --quiet && git diff --cached --quiet; then
  exit 0
fi

# 你要的质量门禁
if command -v golangci-lint &> /dev/null; then
  make lint
else
  echo "⚠️  golangci-lint not installed, skipping lint (brew install golangci-lint)"
fi
make test
make test-integration
make test-integration-cleanup

# 检查是否有关联的 PR，如果有则提示检查 CI 和 review 状态
echo ""
echo "============================================"
if command -v gh &> /dev/null; then
  PR_NUMBER=$(gh pr view --json number -q '.number' 2>/dev/null || echo "")
  if [ -n "$PR_NUMBER" ]; then
    echo "📋 PR #$PR_NUMBER detected"
    echo ""
    echo "After pushing, check merge readiness with:"
    echo "  ./scripts/check-pr-mergeable.sh $PR_NUMBER"
    echo ""
    echo "To merge (after CI passes):"
    echo "  ./scripts/merge-pr.sh $PR_NUMBER"
  else
    echo "💡 No PR found for current branch"
    echo "   Create one with: gh pr create"
  fi
else
  echo "⚠️  gh CLI not installed, skipping PR checks"
fi
echo "============================================"