#!/usr/bin/env bash
set -euo pipefail

# AWS profile to use for all AWS CLI calls in this script
export AWS_PROFILE=debo-locale

AWS_REGION=eu-west-1
AWS_ACCOUNT_ID=135053816219
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
IMAGE_NAME=streamlit-app
IMAGE_TAG=mobile-signal-app
LOCAL_IMAGE=mobile-signal-app

aws ecr get-login-password \
  --region "${AWS_REGION}" | docker login \
  --username AWS \
  --password-stdin "${ECR_REGISTRY}"

docker build -t "${LOCAL_IMAGE}" .

docker run -p 8501:8501 \
  -e AWS_PROFILE="${AWS_PROFILE}" \
  -e AWS_SDK_LOAD_CONFIG=1 \
  -v "${HOME}/.aws:/root/.aws" \
  "${LOCAL_IMAGE}"

#docker tag "${LOCAL_IMAGE}:latest" "${ECR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"

# docker push "${ECR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"