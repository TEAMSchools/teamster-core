GH_WORKLOAD_IDENTITY_POOL_ID=$(
    gcloud iam workload-identity-pools describe github-pool \
    --location=global \
    --format="value(name)" \
    2> /dev/null
)

gcloud iam service-accounts add-iam-policy-binding ${GCP_SERVICE_ACCOUNT} \
    --project=${GCP_PROJECT_ID} \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/${GH_WORKLOAD_IDENTITY_POOL_ID}/attribute.repository/${GH_ORG_NAME}/${IMAGE_NAME}"
