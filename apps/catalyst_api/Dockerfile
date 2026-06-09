# catalyst_api — core-x federal-award read gateway. Runs as its OWN Railway
# project (`catalyst-api`), core-x-owned — NOT nested in any product project.
# It is a SHARED gateway: multiple product BFFs (government-contracted
# platform-app, rare-structure platform-api, …) call it cross-project, so it is
# exposed on a public Railway domain and gated by the CATALYST_API_TOKEN bearer.
# (Railway has no cross-project private networking; a shared gateway cannot be
# private. The bearer token — enforced fail-closed at boot — is the auth boundary.)
#
# Build context is the core-x repo ROOT so the `apps` package resolves; the
# service sets RAILWAY_DOCKERFILE_PATH=apps/catalyst_api/Dockerfile. Doppler CLI
# is copied in so `doppler run` fetches the core-x/prd secrets (R2_* +
# CATALYST_API_TOKEN) at startup using the DOPPLER_TOKEN Railway injects.

FROM dopplerhq/cli:3 AS doppler

FROM python:3.12-slim AS base
COPY --from=doppler /bin/doppler /usr/local/bin/doppler

WORKDIR /app
# Only the self-contained catalyst_api package is needed (no core/ or pipelines/
# imports) — keep the image lean.
COPY apps/__init__.py apps/__init__.py
COPY apps/catalyst_api apps/catalyst_api
RUN pip install --no-cache-dir -r apps/catalyst_api/requirements.txt

# Railway sets $PORT and routes its public edge to the container. HOST=:: binds
# all interfaces (IPv6/dual-stack) so the public edge — and any same-project peer
# — can reach it. Auth is the bearer token, not network isolation.
ENV HOST=:: \
    PORT=8080
EXPOSE 8080
CMD ["doppler", "run", "--", "python", "-m", "apps.catalyst_api.main"]
