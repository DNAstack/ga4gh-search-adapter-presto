# Intro

This branch implements proposals from [Search API Confluence doc](https://dnastack.atlassian.net/wiki/spaces/DISCO/pages/394690561/Search+API)

# Running

```
export SQL_DATASOURCE_ENABLED=false
export SQL_DATASOURCE_URL=UNKNOWN
export SQL_DATASOURCE_USERNAME=UNKNOWN
export SQL_DATASOURCE_PASSWORD=UNKNOWN
export SQL_DATASOURCE_DRIVERCLASSNAME=UNKNOWN

export PRESTO_DATASOURCE_ENABLED=true
export PRESTO_DATASOURCE_URL=jdbc:presto://presto.staging.dnastack.com:443
export PRESTO_DATASOURCE_USERNAME=<user>
export PRESTO_DATASOURCE_PASSWORD=<password>
export PRESTO_RESULTS_LIMIT_MAX=100
export CORS_URLS=http://localhost:4200


mvn spring-boot:run
```


