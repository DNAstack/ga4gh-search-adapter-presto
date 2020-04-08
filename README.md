# Intro

An implementation of the [GA4GH Discovery Search API](https://github.com/ga4gh-discovery/ga4gh-discovery-search), on top of
[PrestoSQL](https://prestosql.io). This software enables users to enumerate and query data surfaced by an instance of PrestoSQL
in a manner compliant with the GA4GH Discovery Search specification, and receive responses compliant with the 
[Table](https://github.com/ga4gh-discovery/ga4gh-discovery-search/blob/develop/TABLE.md) specification.  

# Building

Prerequisites

- Java 11
- tested with Maven 3.5.4


To build the complete project do
```
./mvnw clean install
```

# CI/CD

```bash
#Build docker image for the service
./ci/build-docker-image <image tag>:<image version> ga4gh-search-adapter-presto <image version>

#Build docker image for end-to-end tests
./ci/build-docker-e2e-image <image tag>:<image version>> ga4gh-search-adapter-presto-e2e-image <image version>>

```

# Running

## Quick Start
You can quickly launch the server without authentication requirements, against a similarly anonymously-accessible 
instance of presto, by running it with the following environment variables set: 

|  Environment Variable  |           Example Value  | Notes                                        |
|:----------------------:|:------------------------:|----------------------------------------------|
|  PRESTO_DATASOURCE_URL | https://yourprestoserver | The https address of your presto server.     |
| SPRING_PROFILES_ACTIVE |          no-auth         | Makes the search server anonymously accessible. |

## Auth Profiles

The app can be deployed using one of 3 different spring profiles which configure the authentication expectations. The default profile
will be used if no other profile is activated. to set a profile simply set the `SPRING_PROFILES_ACTIVE` environment variable 
to one of the three profiles outlined below:


#### `default` (Token-Based Authentication)

The default profile requires every inbound request to include an OAuth access token, issued by the configured Security Token Service.  

##### Configuring A Deployment with Token-Based Auth

The built-in defaults are good for local development, but you will need to customize the settings in a real deployment. 
Issuer configuration is easy and can be done from environment variables. One may configure multiple
issuers, using either a `jwkset` endpoint or by passing in an actual `RSA` public key. This flexibility allows for adding
token issuers which do not have a `jwkset` endpoint. The configuration is described by the [AuthConfig](src/main/java/org/ga4gh/discovery/search/security/AuthConfig.java)
class. 

Note that provided values are **sample values only.** Replace the values below with values appropriate to your context. 


```bash

APP_AUTH_TOKENISSUERS_0_ISSUERURI="https://your.expected.issuer.com"
APP_AUTH_TOKENISSUERS_0_JWKSETURI="https://your.expected.issuer/oauth/jwks"
# Set audience if you want your token's audience to be validated.
APP_AUTH_TOKENISSUERS_0_AUDIENCES_0_="ga4gh-search-adapter-presto"
# Set scopes if you want your token's scopes to be validated.
APP_AUTH_TOKENISSUERS_0_SCOPES_0_="read:*"
```

#### `no-auth` (No Authentication)
**DO NOT USE IN PRODUCTION**

This profile will publicly expose all routes and does not require any authentication. This should ONLY be used
for local development and testing.

#### `basic-auth` (Basic Authentication)

This profile will protect API routes with `basic` authentication. Additionally, when a user makes a request, if they have
not logged in they will be redirected to a login screen. The default username is `user`, and the default password is set in
the `application.yaml`.

To configure the username and password simply set following config or environment variables:

```
SPRING_SECURITY_USER_NAME={some-user-name}
SPRING_SECURITY_USER_PASSWORD={some-password}
```

## Presto Setup

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
