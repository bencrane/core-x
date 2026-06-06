# edge_api — PUBLIC Railway service (its OWN new project; NOT rare-structure-hq,
# which is the private award-reader). Build context = core-x repo ROOT so the
# `apps` package resolves; the service sets RAILWAY_DOCKERFILE_PATH=apps/edge_api/Dockerfile.
# Doppler CLI is copied in so `doppler run` fetches core-x/prd secrets at startup
# using the DOPPLER_TOKEN the service injects.
#
# Also runnable on Render (Python buildpack) without this Dockerfile:
#   build:  pip install -r apps/edge_api/requirements.txt
#   start:  python -m apps.edge_api.main   (secrets via Render env / Doppler sync)

FROM dopplerhq/cli:3 AS doppler

FROM python:3.12-slim AS base
COPY --from=doppler /bin/doppler /usr/local/bin/doppler

WORKDIR /app
# Self-contained package — no core/ or pipelines/ imports — keep the image lean.
COPY apps/__init__.py apps/__init__.py
COPY apps/edge_api apps/edge_api
RUN pip install --no-cache-dir -r apps/edge_api/requirements.txt

# PUBLIC service — bind 0.0.0.0 (public ingress), unlike catalyst_api's private
# IPv6 `::`. The platform injects $PORT; default 8080 for a bare local run.
ENV HOST=0.0.0.0
EXPOSE 8080
CMD ["doppler", "run", "--", "python", "-m", "apps.edge_api.main"]
