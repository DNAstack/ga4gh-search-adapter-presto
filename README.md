# Intro

An implementation of the [GA4GH Discovery Search API](https://github.com/ga4gh-discovery/ga4gh-discovery-search), on top of
[PrestoSQL](https://prestosql.io). This software enables users to enumerate and query data surfaced by an instance of PrestoSQL
in a manner compliant with the GA4GH Discovery Search specification, and receive responses compliant with the 
[Table](https://github.com/ga4gh-discovery/ga4gh-discovery-search/blob/develop/TABLE.md) specification.  

# Quick Start
Get started in 30s.
### Prerequisites
- Java 11+
- A presto server you can access anonymously over HTTP(S).

### Build

```
mvn clean package
```

### Configure
 
Set these two environment variables.
```$xslt
PRESTO_DATASOURCE_URL=https://<your-presto-server>
SPRING_PROFILES_ACTIVE=no-auth
```

### Run
```$xslt
java -jar /target/ga4gh-discovery-search-service-1.0-SNAPSHOT.jar
```

# Building (Advanced)

##Prerequisites

- Java 11
- tested with Maven 3.5.4


To build the complete project do
```
./mvnw clean package
```

## Docker Images
```bash
#Build docker image for the service
./ci/build-docker-image <image tag>:<image version> ga4gh-search-adapter-presto <image version>

#Build docker image for end-to-end tests
./ci/build-docker-e2e-image <image tag>:<image version>> ga4gh-search-adapter-presto-e2e-image <image version>>

```

# Running (Advanced)

## Auth Profiles

The app can be deployed using one of 3 different spring profiles which configure the authentication expectations. The default profile
will be used if no other profile is activated. to set a profile simply set the `SPRING_PROFILES_ACTIVE` environment variable 
to one of the three profiles outlined below:


#### `default` (JWT Authentication)

The default profile requires every inbound request to include a JWT, validated by the settings configured below.
The configuration is described by the [AuthConfig](src/main/java/org/ga4gh/discovery/search/security/AuthConfig.java)
class. This is the profile used if no profile is set.

Set the environment variables below, replacing the values below with values appropriate to your context. 

```bash
# (Required) The STS which issued this token.
APP_AUTH_TOKENISSUERS_0_ISSUERURI="https://your.expected.issuer.com"
# (Required) The Json Web Key Set URI (where to find token validation keys)
APP_AUTH_TOKENISSUERS_0_JWKSETURI="https://your.expected.issuer/oauth/jwks"
# (Optional) Set audience if you want your token's audience to be validated.
APP_AUTH_TOKENISSUERS_0_AUDIENCES_0_="ga4gh-search-adapter-presto"
# (Optional) Set scopes if you want your token's scopes to be validated. Set multiple with _SCOPES_1_, SCOPES_2_...
APP_AUTH_TOKENISSUERS_0_SCOPES_0_="read:*"
```

One may alternatively set the token validation key directly by setting the environment variable `APP_AUTH_TOKENISSUERS_1_RSAPUBLICKEY` to the desired key,
and omitting the `JWKSETURI` variable.

#### `no-auth` (No Authentication)
**DO NOT USE IN PRODUCTION**

This profile will publicly expose all routes and does not require any authentication. Best left in your dev environment.

#### `basic-auth` (Basic Authentication)

This profile will protect API routes with `basic` authentication. Additionally, when a user makes a request, if they have
not logged in they will be redirected to a login screen. The default username is `user`, and the default password is set in
the `application.yaml`.

To configure the username and password, set the following environment variables:

```
SPRING_SECURITY_USER_NAME={some-user-name}
SPRING_SECURITY_USER_PASSWORD={some-password}
```

## Presto Source Configuration

There are a number of required configuration properties that need to be set in order to communicate with a presto deployment. 
### Connectivity
Point the service to a presto server by setting the following environment variable:
>PRESTO_DATASOURCE_URL
### Authentication
If your presto instance is also protected, this adapter supports performing OAuth 2.0 Client Credential grants in order 
to retrieve access tokens for its configured Presto instance.

Configuration of the presto auth setup is quite easy and can be done directly through the following environment variables.

```bash
APP_AUTH_PRESTOOAUTHCLIENT_TOKENURI="https://your.sts/oauth/token"
APP_AUTH_PRESTOOAUTHCLIENT_CLIENTID="your-client-id"
APP_AUTH_PRESTOOAUTHCLIENT_CLIENTSECRET="your-client-secret"
APP_AUTH_PRESTOOAUTHCLIENT_AUDIENCE="your-requested-audience"
APP_AUTH_PRESTOOAUTHCLIENT_SCOPES="your space delimited requested scopes"
```
