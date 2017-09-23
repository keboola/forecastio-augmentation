docker pull quay.io/keboola/developer-portal-cli-v2:latest
export REPOSITORY=`docker run --rm \
  -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME \
  -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD \
  quay.io/keboola/developer-portal-cli-v2:latest ecr:get-repository keboola keboola.ag-forecastio`
docker tag keboola/forecastio-augmentation:latest $REPOSITORY:$TRAVIS_TAG
docker tag keboola/forecastio-augmentation:latest $REPOSITORY:latest
eval $(docker run --rm \
  -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME \
  -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD \
  quay.io/keboola/developer-portal-cli-v2:latest ecr:get-login keboola keboola.ag-forecastio)
docker push $REPOSITORY:$TRAVIS_TAG
docker push $REPOSITORY:latest

docker run --rm \
  -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME \
  -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD \
  quay.io/keboola/developer-portal-cli-v2:latest update-app-repository keboola keboola.ag-forecastio $TRAVIS_TAG

