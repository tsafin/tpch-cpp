Repository Git Rules

- Never use `git add -A` or `git add .` to stage changes.
- Stage only specific files: `git add path/to/file` or use `git add -p` to interactively select hunks.
- Review changes before committing: `git status --porcelain`, `git diff` and `git diff --staged`.
- Keep commits focused and small; prefer descriptive commit messages.
- If you accidentally commit unwanted files, use `git reset --mixed HEAD~1` and then selectively stage the intended files.
- When in doubt, ask before running broad git commands.
