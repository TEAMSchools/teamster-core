if [ -z "$1" ];
then
    :
else
    source ./env/.env
    envsubst < ./env/$1.env.tmpl > ./env/$1.env
fi
