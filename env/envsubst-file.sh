source ./env/.env
envsubst < ./env/$1.env.tmpl > ./env/$1.env