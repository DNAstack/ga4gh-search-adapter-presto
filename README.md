# Intro

This branch implements the [GA4GH Search API](https://github.com/DNAstack/ga4gh-discovery-search-apis) The frontend will be separated to different repository.

# Building

Prerequisites

- Java 1.8 (Newer versions do not work)
- tested with Maven 3.5.4


To build the complete project do
```
./mvnw clean install
```

# CI/CD

```bash
#Build docker image
./ci/build-docker-image gcr.io/dnastack-container-store/ga4gh-search-adapter-presto:$(git describe) ga4gh-search-adapter-presto $(git describe)

#Build docker
./ci/build-docker-e2e-image gcr.io/dnastack-container-store/ga4gh-search-adapter-presto-e2e-image:$(git describe) ga4gh-search-adapter-presto-e2e-image $(git describe)

```

# Running

## Auth Profiles

The app can be deployed using one of 3 different spring profiles which toggles authentication on or off. The default profile
will be used if no other profile is activated. to set a profile simply set the spring config variable `spring.profiles.active={desired profile}`


#### `default`

The default profile will protect API routes with `bearer-only` authentication. This means that all requests must contain a signed
JWT from one of the configured issuers. You can additionally set required scopes or audiences which will be used when validating
the token claims.

Issuer configuration is easy and can be done from environment variables. Additionally, the api allows you to configure multiple
issuers using either a `jwkset` endpoint or by passing in an actual `RSA` public key. This flexibility allows for adding
token issuers which do not have a `jwkset` endpoint, such as the `DAM`. The configuration is described by the [AuthConfig](src/main/java/org/ga4gh/discovery/search/security/AuthConfig.java)
class. 


```bash

APP_AUTH_TOKENISSUERS_0_ISSUERURI="https://wallet.staging.dnastack.com"
APP_AUTH_TOKENISSUERS_0_JWKSETURI="https://wallet.staging.dnastack.com/oauth/jwks"
# Set audience if you want to restrict audience
APP_AUTH_TOKENISSUERS_0_AUDIENCES_0_="ga4gh-search-adapter-presto"
# Set scopes if you want to restrict by scope
APP_AUTH_TOKENISSUERS_0_SCOPES_0_="read:*"

```

#### `no-auth`
**DO NOT USE IN PRODUCTION**

This profile will publicly expose all routes and does not require any authentication. This should ONLY be used
for local development and testing.

#### basic-auth 

This profile will protect API routes with `basic` authentication. Additionally, when a user makes a request, if they have
not logged in they will be redirected to a login screen. The default username is `user`, and the default password is set in
the `application.yaml`.

To configure the username and password simply set following config or environment variables:

```
SPRING_SECURITY_USER_NAME={some-user-name}
SPRING_SECURITY_USER_PASSWORD={some-password}
```




## Presto Setup

There is a number of required configuration properties that need to be set in order to communicate with a presto deployment. 
Presto now requires a JWT signed by a valid issuer, in most cases this issuer will be a `wallet` deployment such as 
https://wallet.prod.dnastack.com or https://wallet.staging.dnastack.com. To facilitate this, the search adapter must be
a `client` of the target `token` issuer, and it will use the `client_credential` flow in order to retrieve an `access_token`

Configuration of the presto auth setup is quite easy and can be done directly through environment variables.

```bash
APP_AUTH_PRESTOOAUTHCLIENT_TOKENURI="https://wallet.staging.dnastack.com/oauth/token"
APP_AUTH_PRESTOOAUTHCLIENT_CLIENTID="some-client"
APP_AUTH_PRESTOOAUTHCLIENT_CLIENTSECRET="your-client-secret"
APP_AUTH_PRESTOOAUTHCLIENT_AUDIENCE="who-the-token-is-for"

# By default yuou will get ALL scopes that client is authorized for, if you would like to request a downscoped
# token, then set the following ENV variable
APP_AUTH_PRESTOOAUTHCLIENT_SCOPES="list of scopes separated by a space"

```


## Query History Encryption at Rest

In certain circumstances, the queries themselves might contain sensitive data which should not be exposed when persisted to the
backing database. In these instances the Search API provides a simple way for encrypting users search history 
using [JWE](https://tools.ietf.org/html/rfc7516). JWE is designed to be fast and lightweight and provides both security as well
as integrity checks of the encrypted data.

To begin, you first must generate and RSA Private key in `PEM` format using OpenSSL

```bash
openssl genrsa -out rsa-key.pem 2048
``` 

Once you have generated the key you can set an environment variable with its contents to enable Encryption and Decryption of search keys

```
APP_DATA_ENCRYPTION_RSAKEYPAIR=$(cat rsa-key.pem)
```

**IMPORTANT**

Once fields are encrypted, the ONLY way for them to be unencrypted is to supple the appropriate key at startup. If the key is lost
there is no method on Gods green earth to retrieve it by. Additionally, the `EncryptionService` is designed to fail silently. That is,
if it cannot encrypt or decrypt a field, the original string will be returned. In the case of a key change (without planned migration),
this means that all previously encrypted search histories will be returned in their compact JWE form

