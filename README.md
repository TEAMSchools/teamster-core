# teamster
Next-gen data orchestration, powered by Dagster

- Private GKE Autopilot cluster using Cloud NAT
- Base Docker container w/ common ops, graphs, and resources
- [Google Artifact Registry](https://cloud.google.com/artifact-registry/docs/docker/store-docker-container-images)
- Google Cloud services access prodivded by [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
- PDM dependency management

`.env`
```
IMAGE_NAME=
DAGSTER_CLOUD_AGENT_TOKEN_PROD=
DAGSTER_CLOUD_AGENT_TOKEN_STG=
PROJECT_ID=
GCP_REGION=
```
