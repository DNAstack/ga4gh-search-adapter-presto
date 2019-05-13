package org.ga4gh.discovery.search.rest;

import static com.google.common.base.Preconditions.checkArgument;

import java.sql.SQLException;

import org.ga4gh.discovery.search.serde.QueryDeserializer;
import org.ga4gh.discovery.search.source.SearchSource;
import org.ga4gh.discovery.search.source.presto.PrestoAdapterImpl;
import org.ga4gh.discovery.search.source.presto.PrestoSearchSource;
import org.ga4gh.discovery.search.source.sql.SQLSearchSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import io.prestosql.sql.tree.Query;

@Configuration
public class ApplicationConfig {

    /** MySQL */
    @Value("${sql.datasource.enabled}")
    private boolean sqlEnabled;

    @Value("${sql.datasource.url}")
    private String sqlDatasourceUrl;

    @Value("${sql.datasource.username}")
    private String sqlDatasourceUsername;

    @Value("${sql.datasource.password}")
    private String sqlDatasourcePassword;

    @Value("${sql.datasource.driver-class-name}")
    private String sqlDatasourceDriverClassName;

    /** PrestoSQL */
    @Value("${presto.datasource.enabled}")
    private boolean prestoEnabled;

    @Value("${presto.datasource.url}")
    private String prestoDatasourceUrl;

    @Value("${presto.datasource.username}")
    private String prestoDatasourceUsername;

    @Value("${presto.datasource.password}")
    private String prestoDatasourcePassword;

    /** Other settings */
    @Value("${cors.urls}")
    private String corsUrls;

    @Value("${security.enabled:true}")
    private String securityEnabled;

    @Bean
    public SearchSource searchSource() throws ClassNotFoundException, SQLException {
        checkArgument(
                !(sqlEnabled && prestoEnabled), "SQL and Presto datasource can't be both enabled");
        checkArgument(sqlEnabled || prestoEnabled, "At least one datasource has to be enabled");

        return prestoEnabled ? getPrestoSearchSource() : getSqlSearchSource();
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
                new PrestoAdapterImpl(
                        prestoDatasourceUrl, prestoDatasourceUsername, prestoDatasourcePassword));
    }

    @Bean
    public Jackson2ObjectMapperBuilder objectMapperBuilder() {
        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
        builder.deserializerByType(Query.class, new QueryDeserializer());
        return builder;
    }

    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {

            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**")
                        .allowedOrigins(parseCorsUrls())
                        .allowCredentials(true)
                        .allowedHeaders("*")
                        .allowedMethods("*");
            }
        };
    }

    @Bean
    public WebSecurityConfigurerAdapter securityConfigurer() {
        if ("false".equals(securityEnabled)) {
            return new WebSecurityConfigurerAdapter() {
                @Override
                protected void configure(HttpSecurity http) throws Exception {
                    http.authorizeRequests().anyRequest().permitAll().and().csrf().disable();
                }
            };
        } else {
            return new WebSecurityConfigurerAdapter() {
                @Override
                protected void configure(HttpSecurity http) throws Exception {
                    http.authorizeRequests()
                            .antMatchers("/api/**")
                            .authenticated()
                            .and()
                            .httpBasic()
                            .and()
                            .authorizeRequests()
                            .antMatchers("/actuator/health", "/actuator/info")
                            .permitAll()
                            .and()
                            .authorizeRequests()
                            .anyRequest()
                            .authenticated()
                            .and()
                            .formLogin()
                            .and()
                            .csrf()
                            .disable();
                }
            };
        }
    }

    private String[] parseCorsUrls() {
        return corsUrls.split(",");
    }
}
