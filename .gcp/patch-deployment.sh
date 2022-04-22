# get pod name
DEPLOYMENT_NAME=$(kubectl get deployment --namespace dagster-cloud -l location_name=${INSTANCE_NAME} -o name)

# export current deployment yaml
kubectl get ${DEPLOYMENT_NAME} --namespace dagster-cloud -o yaml > ./.gcp/deployment-old.yaml

# generate patch yaml from template
envsubst < ./.gcp/deployment-patch.yaml.tmpl > ./.gcp/deployment-patch.yaml

# merge in updated deployment yaml
kubectl patch deployment ${DEPLOYMENT_NAME} --namespace dagster-cloud --patch-file ./.gcp/deployment-patch.yaml

# export updated deployment yaml
kubectl get ${DEPLOYMENT_NAME} --namespace dagster-cloud -o yaml > ./.gcp/deployment-new.yaml
