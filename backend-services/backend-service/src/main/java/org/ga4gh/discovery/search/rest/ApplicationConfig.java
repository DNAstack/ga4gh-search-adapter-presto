/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.rest;

import java.sql.SQLException;
import org.ga4gh.discovery.search.query.QueryRule;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.rest.serialize.QueryRuleDeserializer;
import org.ga4gh.discovery.search.rest.serialize.SearchQueryDeserializer;
import org.ga4gh.discovery.search.source.SearchSource;
import org.ga4gh.discovery.search.source.presto.PrestoSearchSource;
import org.ga4gh.discovery.search.source.sql.SQLSearchSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

@Configuration
public class ApplicationConfig {
    
    /*
     * MySQL
    */
    @Value( "${sql.datasource.url}" )
    private String sqlDatasourceUrl;
    
    @Value( "${sql.datasource.username}" )
    private String sqlDatasourceUsername;
    
    @Value( "${sql.datasource.password}" )
    private String sqlDatasourcePassword;
    
    @Value( "${sql.datasource.driver-class-name}" )
    private String sqlDatasourceDriverClassName;
    
    /**
     * Presto
     */
    @Value( "${presto.datasource.url}" )
    private String prestoDatasourceUrl;
    
    @Value( "${presto.datasource.username}" )
    private String prestoDatasourceUsername;
    
    @Value( "${presto.datasource.password}" )
    private String prestoDatasourcePassword;
    
    @Value( "${presto.datasource.driver-class-name}" )
    private String prestoDatasourceDriverClassName;
    
    @Bean
    public SearchSource searchSource() throws ClassNotFoundException, SQLException {
        //return getSqlSearchSource();
        return getPrestoSearchSource();
    }

    private SearchSource getSqlSearchSource() {
        return new SQLSearchSource(
                sqlDatasourceUrl,
                sqlDatasourceUsername,
                sqlDatasourcePassword, 
                sqlDatasourceDriverClassName);
    }

    private SearchSource getPrestoSearchSource() {
        return new PrestoSearchSource(
                prestoDatasourceUrl,
                prestoDatasourceUsername,
                prestoDatasourcePassword, 
                prestoDatasourceDriverClassName);
    }
    
    @Bean
    public Jackson2ObjectMapperBuilder objectMapperBuilder() {
        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
        builder.deserializerByType(SearchQuery.class, new SearchQueryDeserializer());
        builder.deserializerByType(QueryRule.class, new QueryRuleDeserializer());
        return builder;
    }
    
}