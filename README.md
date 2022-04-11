# teamster-core
Next-gen data orchestration, powered by Dagster

- Private GKE Autopilot cluster using Cloud NAT
- Docker container w/ common ops, graphs, and resources
- Dagster Cloud's local agent on your staging deployment for local development
- [Google Artifact Registry](https://cloud.google.com/artifact-registry/docs/docker/store-docker-container-images)
- Google Cloud services access prodivded by [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
- [PDM](https://pdm.fming.dev/) for dependency management and build scripts

`.env`
```
IMAGE_NAME=
PROJECT_ID=
GCP_REGION=
IMAGE_URI=${GCP_REGION}-docker.pkg.dev/${PROJECT_ID}/${IMAGE_NAME}/${IMAGE_NAME}:latest
DAGSTER_CLOUD_AGENT_TOKEN_PROD=
DAGSTER_CLOUD_AGENT_TOKEN_STG=
```
