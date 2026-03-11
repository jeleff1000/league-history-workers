# League History Workers

GitHub Actions workflows for [League History](https://leaguehistory.app) data import and processing.

This is a public runner repo — workflows check out code from the private source repository at runtime.

## Setup

Required secrets:
- `PRIVATE_REPO_PAT` — Fine-grained PAT with `contents: read` on the private repo
- `YAHOO_CLIENT_ID` / `YAHOO_CLIENT_SECRET`
- `MOTHERDUCK_TOKEN`
- `CREDENTIAL_ENCRYPTION_KEY`
