# catalyst_api — Railway service, co-located with the rare-structure-hq
# platform-api BFF for PRIVATE networking (no public domain; the BFF reaches it
# at http://catalyst-api.railway.internal:8080).
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

# Private-only service. HOST=:: binds IPv6 (Railway's private network is
# IPv6-only — binding 0.0.0.0 would be unreachable from the BFF). PORT is fixed
# so the BFF can target a stable catalyst-api.railway.internal:8080.
ENV HOST=:: \
    PORT=8080
EXPOSE 8080
CMD ["doppler", "run", "--", "python", "-m", "apps.catalyst_api.main"]
