#!/usr/bin/env bash
set -euo pipefail

# AWS profile to use for all AWS CLI calls in this script
export AWS_PROFILE=debo-locale

aws ecr get-login-password \
  --region eu-west-1 | docker login \
  --username AWS \
  --password-stdin 135053816219.dkr.ecr.eu-west-1.amazonaws.com

docker build --provenance=false -t "entity-booking-app" .

docker run -p 8501:8501 \
  -e AWS_PROFILE="debo-locale" \
  -e AWS_SDK_LOAD_CONFIG=1 \
  -v "${HOME}/.aws:/root/.aws" \
  "entity-booking-app"

docker tag entity-booking-app:latest 135053816219.dkr.ecr.eu-west-1.amazonaws.com/streamlit-app:entity-booking-app

docker push 135053816219.dkr.ecr.eu-west-1.amazonaws.com/streamlit-app:entity-booking-app