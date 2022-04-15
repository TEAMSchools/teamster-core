# set up Workload Identity Federation for GitHub actions

# create WI pool
gcloud iam workload-identity-pools create "github-pool" \
    --project=${GCP_PROJECT_ID} \
    --location="global" \
    --display-name="GitHub Pool"

# get pool ID
WORKLOAD_IDENTITY_POOL_ID=$(gcloud iam workload-identity-pools describe "github-pool" --project="${GCP_PROJECT_ID}" --location="global" --format="value(name)")

# create WI provider for pool
gcloud iam workload-identity-pools providers create-oidc "github-provider" \
    --project="${GCP_PROJECT_ID}" \
    --location="global" \
    --workload-identity-pool="github-pool" \
    --display-name="GitHub Provider" \
    --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
    --issuer-uri="https://token.actions.githubusercontent.com"

# assign WI role to service account
gcloud iam service-accounts add-iam-policy-binding "${GCP_SERVICE_ACCOUNT}" \
    --project=${GCP_PROJECT_ID} \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/${WORKLOAD_IDENTITY_POOL_ID}/attribute.repository/${ORG_NAME}/${IMAGE_NAME}"

# output WI provider name to save as ENV
WORKLOAD_IDENTITY_PROVIDER=$(gcloud iam workload-identity-pools providers describe "github-provider" --project="${GCP_PROJECT_ID}" --location="global" --workload-identity-pool="github-pool" --format="value(name)")
echo ${WORKLOAD_IDENTITY_PROVIDER}
