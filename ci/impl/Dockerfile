# This is a two stage build, as described in the Docker docs: https://docs.docker.com/develop/develop-images/multistage-build/

##
## Stage 1: Build and test the Java Sources
##

FROM openjdk:8-jdk-slim AS build
ARG APP_NAME
ARG APP_VERSION
WORKDIR /build

# Before bringing in project sources, resolve maven dependencies.
# This allows Docker to reuse the cached image with all dependencies resolved.

ADD target/pom.xml /build/pom.xml
ADD target/.mvn/ /build/.mvn/
ADD target/mvnw /build/mvnw
ADD target/backend/service/pom.xml /build/backend/service/pom.xml
ADD target/backend/data-sources/presto-source/pom.xml /build/backend/data-sources/presto-source/pom.xml
ADD target/backend/data-models/pom.xml /build/backend/data-models/pom.xml
ADD target/frontend/pom.xml /build/frontend/pom.xml

RUN ./mvnw -B de.qaware.maven:go-offline-maven-plugin:resolve-dependencies
RUN ./mvnw -B versions:set -DnewVersion=${APP_VERSION}

# Now add sources, which will bust the cache.
ADD target/backend/service/src/ /build/backend/service/src/
ADD target/backend/data-sources/presto-source/src/ /build/backend/data-sources/presto-source/src/
ADD target/backend/data-models/src/ /build/backend/data-models/src/
ADD target/frontend/demo/ /build/frontend/demo/

# If this step fails with an error like
#     Failed to execute goal org.apache.maven.plugins:maven-surefire-plugin:2.22.1:test
#     (default-test) on project xxx: Unable to generate classpath:
#     org.apache.maven.artifact.resolver.MultipleArtifactsNotFoundException: Missing:
# then update the version number of surefire-junit4 in pom.xml under the
# dynamicDependencies section of go-offline-maven-plugin configuration.
RUN ./mvnw -B -o package

##
## Stage 2: Package the runnable image
##

# This stage uses a JRE rather than JDK because it doesn't have to build anything; it just runs the fat jar
# produced by the previous stage.
FROM openjdk:8-slim
ARG APP_NAME
ARG APP_VERSION

COPY --from=0 /build/backend/service/target/ga4gh-discovery-search-service-*.jar /app.jar
ADD entrypoint.sh /entrypoint.sh
EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
