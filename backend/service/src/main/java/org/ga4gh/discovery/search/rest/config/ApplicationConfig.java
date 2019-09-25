package org.ga4gh.discovery.search.rest.config;

import io.prestosql.sql.tree.Query;
import org.ga4gh.discovery.search.serde.QueryDeserializer;
import org.ga4gh.discovery.search.source.SearchSource;
import org.ga4gh.discovery.search.source.presto.PrestoAdapterImpl;
import org.ga4gh.discovery.search.source.presto.PrestoSearchSource;
import org.ga4gh.discovery.search.source.presto.SchemaService;
import org.ga4gh.discovery.search.source.presto.ServiceAccountAuthenticator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class ApplicationConfig {

    @Value("${presto.datasource.url}")
    private String prestoDatasourceUrl;

    @Value("${presto.datasource.username}")
    private String prestoDatasourceUsername;

    @Value("${presto.auth.token-url}")
    private String prestoTokenUrl;

    @Value("${presto.auth.client-id}")
    private String prestoClientId;

    @Value("${presto.auth.client-secret}")
    private String prestoClientSecret;

    @Value("${presto.auth.scope}")
    private String prestoRequiredScopes;

    @Value("${presto.auth.audience}")
    private String prestoAudiences;

    /** Other settings */
    @Value("${cors.urls}")
    private String corsUrls;

    @Value("${security.enabled:true}")
    private String securityEnabled;

    @Bean
    public ServiceAccountAuthenticator getServiceAccountAuthenticator(){
        return new ServiceAccountAuthenticator(prestoClientId,prestoClientSecret,prestoRequiredScopes,prestoAudiences,prestoTokenUrl);
    }

    @Bean
    public SearchSource getPrestoSearchSource(ServiceAccountAuthenticator accountAuthenticator) {
        return new PrestoSearchSource(new PrestoAdapterImpl(prestoDatasourceUrl, prestoDatasourceUsername, accountAuthenticator));
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
                            .antMatchers("/actuator/health", "/actuator/info", "/service-info")
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
