# Deploying the gateway processes

Units, a launcher and an image definition for `oe-gateway`, `oe-stream`, `oe-marketdata` and `oe-projector` on
hosts you manage. The unit files are the same on every host.

| Path | What |
|---|---|
| `systemd/oe-worker@.service` | a process with the external media driver (production): `oe-worker@oe-gateway.service` |
| `systemd/oe-worker-embedded@.service` | a process with the driver inside it (development and demo) |
| `bin/worker-launch` | sets the process's driver mode from the unit, refuses an env file that says otherwise, execs it |
| `env/process.env.example` | the env file layout |
| `oci/Dockerfile`, `oci/smoke.sh` | the image (published binaries on ubuntu:24.04, non-root, read-only root) and its smoke test |
| `render-verify.sh` | renders three hosts and checks the result |

The external media driver unit, `aeronmd@.service`, ships with cluster-tools (`deploy/systemd`).

## Install

1. Create the `openexchange` system user (the processes and their drivers share it).
2. Put the published binaries at `/opt/openexchange/gateway/` and `bin/worker-launch` at
   `/opt/openexchange/gateway/bin/worker-launch` (0755). For the external driver, install the cluster-tools set and
   its `aeronmd@.service`.
3. Settings in `/etc/openexchange/gateway/<process>.env`: `GATEWAY_*`, `STREAM_*`, `MD_*`, `HISTORY_PROJECTOR_*`;
   the process checks every one and exits 78 on any problem. Secrets in `<process>.secrets.env` (root:root 0600) or
   in the files the settings name.
4. `systemctl enable --now oe-worker@oe-gateway.service` (enabling links it into `aeronmd@oe-gateway.service.wants/`).

## Restarts

`Restart=on-failure`, backoff 2 s to 60 s, `RestartPreventExitStatus=78` (a refused setting is fixed by an operator,
not by a restart). External: the unit `BindsTo=` and is ordered `After=` `aeronmd@<process>`, stops with it, and is
`WantedBy=` it, so a driver that systemd restarts brings it back. cgroup limits go in a per-host drop-in.

## Image

```sh
docker build -f deploy/oci/Dockerfile --build-context deploy=deploy -t openexchange/gateway <published binaries dir>
docker run --read-only --tmpfs /tmp --env-file oe-gateway.env openexchange/gateway \
    /opt/openexchange/gateway/bin/worker-launch external oe-gateway
```

With the external driver, run the cluster-tools image's aeronmd and share its network namespace, its driver
directory (`AERON_DIR`) and its uid (10001).

## Tests

- `deploy/render-verify.sh [--cluster-tools <checkout>] [--self-test]`: unit identity across three hosts, restart
  and driver wiring, `systemd-analyze verify` on every instance (against the real driver unit when a cluster-tools
  checkout is given), and the launcher (syntax, shellcheck, refusals, the command it execs). `--self-test` runs the
  negative controls.
- `deploy/oci/smoke.sh --set <published binaries dir>`: builds the image and requires 78 from every process started
  without settings, from a contradicting driver mode and from no command.
