#!/usr/bin/env bash
set -euo pipefail

cd "${CLAUDE_PROJECT_DIR:-.}"

# 只有当工作区/暂存区有变化时才跑，避免每次 Stop 都跑一遍
if git diff --quiet && git diff --cached --quiet; then
  exit 0
fi

# 你要的质量门禁
make lint
make test
make test-integration
make test-integration-cleanup