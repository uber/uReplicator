container:
	mvn clean package -DskipTests
	docker build -t ureplicator .
