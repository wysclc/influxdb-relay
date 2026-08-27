# Repository Instructions

## Git Workflow

- After each completed code change in this repo, create a git commit automatically.
- After each successful commit, push the current branch automatically.
- Use small, focused commits.
- Use rich multi-line commit messages so `git log` is the primary step-by-step history for this repo.
- Prefer Chinese for generated documentation, user-facing text, and code comments; use English as the fallback when Chinese is unsuitable, while preserving the programming language syntax and the repository's identifier conventions.
- Commit messages should use:
  - a short imperative subject line
  - a blank line
  - concise body sections such as `Request:`, `Changes:`, `Verification:`, and `Next useful context:` when relevant
- Do not wait for the user to ask for a commit.
- Before committing, run the smallest relevant verification that can reasonably detect errors in the current change. If verification cannot be run, record the reason in the commit message.
- Do not mix unrelated dirty files into the current commit. If unrelated changes are clearly scoped, complete, and safe to verify, verify and commit them separately with an accurate message; otherwise leave them untouched and report them to the user.
