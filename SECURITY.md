# Security operations

## Credentials

Paper mode does not require wallet credentials. For live mode, prefer injecting
`POLY_PRIVATE_KEY` from a process supervisor or secret manager instead of
storing it on disk. Never paste a private key into an issue, terminal capture,
dashboard mirror, or committed file.

If `.env` is used, keep it beside `config.py`, never commit it, and restrict it
to the bot account before starting:

```bash
chmod 600 .env
```

On Windows PowerShell (replace the path and account name as needed):

```powershell
icacls .env /inheritance:r
icacls .env /grant:r "$env:USERNAME:(R,W)"
```

The repository ignore rules cover `.env`, keys, ledgers, journals, locks, and
logs. Ignore rules do not protect files already added to version control; remove
any such file from the index and rotate every exposed credential.

## Logs and terminal output

Dashboard events are bounded, credential-redacted, and stripped of terminal
control characters. Optional dashboard mirror logs are opened as regular,
non-symlink, non-inheritable files and use mode `0600` on POSIX. State ledgers,
order journals, exit logs, and terminal captures still contain sensitive wallet
activity and should be retained and shared on a least-privilege basis.

## Network endpoints

Authenticated CLOB traffic defaults to `https://clob.polymarket.com`. A custom
origin is rejected unless `ALLOW_CUSTOM_CLOB_HOST=1` is explicitly set, and
even then it must be an HTTPS origin with no embedded credentials, path, query,
or fragment. Enabling a custom endpoint gives that server access to signed and
authenticated request material; use only infrastructure you control.

## Dependency and release checks

Before a production release, install in a clean virtual environment, run
`python -m pip check`, audit the resolved dependency tree with `pip-audit`, and
run every deterministic test listed in `README.md`. The source requirements use
compatibility ranges rather than a hash-locked, platform-specific release lock;
record and review the exact resolved versions used for each deployment.
