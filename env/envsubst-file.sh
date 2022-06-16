#! /bin/bash

if [[ -z $1 ]]; then
	:
else
	envsubst <./env/"$1".env.tmpl >./env/"$1".env
fi
