# Agent instructions

Read [README.md](README.md) and [docs/contract.md](docs/contract.md) before changing code. The runtime uses the Node standard library only, with zero dependencies. Run `npm run verify` before pushing. A store schema change needs an upgrade path from the previous version and a test that opens it.

## Pull requests: the author merges

The owner's standing rule (2026-11-25) for this repository and its sibling
(`agent-watch-runtime` / `agent-watch-private-adapters`): the agent that opens
a pull request owns it until it merges.

1. Open the pull request from a feature branch. Never push to `main` directly.
2. Watch it until every check on the current head passes, including the
   `quality-gate` job where it runs and the automatic Claude review where it is
   configured. Fix failures at the root cause. Never skip, disable or weaken a
   check or test to get green.
3. Answer review findings. Fix what is real, and reply on the thread when a
   finding does not apply.
4. When the head is green and has no open "Request changes" review, merge it
   yourself. Do not wait for, or ask, the owner to merge.
5. Report the merged result and anything left undone to the owner.

This rule covers these two repositories only. It does not authorize
force-pushing, rewriting history, deleting branches, or merging anyone else's
pull request.
