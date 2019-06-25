# Intro

This branch implements proposals from [Search API Confluence doc](https://dnastack.atlassian.net/wiki/spaces/DISCO/pages/394690561/Search+API)

## Plans

This repository will be gradually transformed so that the backend service will implement [GA4GH Search API](https://github.com/DNAstack/ga4gh-discovery-search-apis) The frontend will be separated to different repository.

# Building

Prerequisites

- Java 1.8
- tested with Maven 3.5.4


To build the complete project including frontend, do 

```
mvn clean install
```

Frontend build may fail, because it uses Angular/NPM libraries external to the maven system and depends heavily on local system config. To build only backend do

```
mvn clean install -pl '!:ga4gh-discovery-search-frontend'
```

# Running

## Using staging presto

To run using staging presto deployment, do (see [presto credentials here](https://dnastack.atlassian.net/wiki/spaces/DISCO/pages/362283009/PrestoSQL#PrestoSQL-PrestoinstanceinStaging)) in the `backend/service` sub-folder:

```
export PRESTO_DATASOURCE_URL=jdbc:presto://presto.staging.dnastack.com:443
export PRESTO_DATASOURCE_USERNAME=<user>
export PRESTO_DATASOURCE_PASSWORD=<pass>
export PRESTO_RESULTS_LIMIT_MAX=100
export CORS_URLS='*'
export SPRING_SECURITY_USER_NAME=user
export SPRING_SECURITY_USER_PASSWORD=user
export SEARCH_API_URL="/api" 
export SECURITY_ENABLED=false

mvn spring-boot:run -Dspring-boot.run.arguments=--logging.level.org.ga4gh=DEBUG
```

for debug mode do

```
mvn spring-boot:run -Dspring-boot.run.arguments=--logging.level.org.ga4gh=DEBUG -Dspring-boot.run.jvmArguments="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
```

## Using local presto

this is more complicated, you need to [download](https://prestosql.io/download.html) Presto, [configure](https://prestosql.io/docs/current/installation/deployment.html) it, add at least one connector (e.g. the built-in [PostgreSQL](https://prestosql.io/docs/current/connector/postgresql.html)) and run.

NOTE: example configs in PrestoSQL documentation uses the port 8080, which clashes with our local backend port

# Example queries

## GET /fields

This will return fields of tables that are avaiable in the demo backend.

## POST /search

### SQL Query

Post following content to `/search` endpoint

```json
{
  "query": "SELECT id, name FROM files LIMIT 10"
}
```

e.g. by running

```bash
curl -d '{"query":"SELECT id, name FROM files LIMIT 10"}' http://localhost:8080/api/search -H "Content-type: application/json"
```

### JSON Query

Post followng content to `/search` endpoint

```json
{
	"json_query": {
		"select": [
			{
				"field": "id"
			},
			{
				"field": "name"
			}
		],
		"from": [
			{
				"table": "files"
			}
		],
		"limit": 10
	}
}
```

e.g. by running

```bash
curl -d '{"json_query":{"select":[{"field":"id"},{"field":"name"}],"from":[{"table":"files"}],"limit":10}}' http://localhost:8080/api/search -H "Content-type: application/json"
```


