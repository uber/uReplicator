REPO=660610034966.dkr.ecr.us-east-1.amazonaws.com/mistsys/ureplicator
VERSION=master-v2
.PHONY: build
all: build push
build:
	docker build -t ${REPO}:${VERSION} .
push:
	aws-okta exec mist-admin -- aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 660610034966.dkr.ecr.us-east-1.amazonaws.com
	aws-okta exec mist-admin -- docker push ${REPO}:${VERSION}