#!/bin/bash

docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag keboola/forecastio-augmentation quay.io/keboola/forecastio-augmentation:$TRAVIS_TAG
docker images
docker push quay.io/keboola/forecastio-augmentation:$TRAVIS_TAG
