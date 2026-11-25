# syntax=docker/dockerfile:1
# SPDX-License-Identifier: Apache-2.0
#
# The gateway runtime image: the published binaries (oe-gateway, oe-stream, oe-marketdata, oe-projector) on the
# distribution they are built for, with deploy/bin/worker-launch. Nothing is compiled here: the image wraps the
# published artifacts, as a host install does. The external media driver is the cluster-tools image's aeronmd.
#
# Build context: a directory holding the published binaries; deploy/ of this repo is a named context:
#
#   docker build -f deploy/oci/Dockerfile --build-context deploy=deploy -t openexchange/gateway <bin-dir>
#
# Run (read-only root, non-root; the command names the driver mode and the process):
#   docker run --read-only --tmpfs /tmp --env-file oe-gateway.env <image> \
#       /opt/openexchange/gateway/bin/worker-launch external oe-gateway
# With the external driver, share the driver container's network namespace (--network container:<driver>) and its
# driver directory (a shared tmpfs volume, AERON_DIR in both), same uid.

# ubuntu:24.04: the binaries need glibc 2.38 and GLIBCXX 3.4.32; libuuid, libstdc++ and taskset are in the base.
ARG BASE_IMAGE=ubuntu:24.04@sha256:562111b88caa79c71391c0c4d3c2114a7d5c35915c20561fd893cb1e07929e6b
FROM ${BASE_IMAGE}
ARG OE_UID=10001
RUN groupadd --system --gid "${OE_UID}" openexchange \
    && useradd --system --uid "${OE_UID}" --gid "${OE_UID}" --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin openexchange
COPY --chmod=0555 . /opt/openexchange/gateway/
COPY --from=deploy --chmod=0555 bin/worker-launch /opt/openexchange/gateway/bin/worker-launch
ENV GW_HOME=/opt/openexchange/gateway
USER openexchange
# No default process: the command names it. Without one the launcher refuses with its usage (78).
CMD ["/opt/openexchange/gateway/bin/worker-launch"]
