package org.ga4gh.discovery.search.rest.config;

import io.prestosql.sql.tree.Query;
import java.util.ArrayList;
import java.util.List;
import org.ga4gh.discovery.search.model.serde.QueryDeserializer;
import org.ga4gh.discovery.search.model.source.SearchSource;
import org.ga4gh.discovery.search.rest.security.AuthConfig;
import org.ga4gh.discovery.search.rest.security.AuthConfig.IssuerConfig;
import org.ga4gh.discovery.search.rest.security.AuthConfig.OauthClientConfig;
import org.ga4gh.discovery.search.rest.security.DelegatingJwtDecoder;
import org.ga4gh.discovery.search.source.presto.PrestoAdapterImpl;
import org.ga4gh.discovery.search.source.presto.PrestoSearchSource;
import org.ga4gh.discovery.search.source.presto.ServiceAccountAuthenticator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;


@EnableWebSecurity
@Configuration
public class ApplicationConfig {

    @Value("${presto.datasource.url}")
    private String prestoDatasourceUrl;

    @Value("${presto.datasource.username}")
    private String prestoDatasourceUsername;


    /**
     * Other settings
     */
    @Value("${cors.urls}")
    private String corsUrls;

    @Bean
    public ServiceAccountAuthenticator getServiceAccountAuthenticator(AuthConfig authConfig) {
        OauthClientConfig clientConfig = authConfig.getPrestoOauthClient();
        return new ServiceAccountAuthenticator(clientConfig.getClientId(), clientConfig
            .getClientSecret(), clientConfig
            .getAudience(), clientConfig.getTokenUri(),clientConfig.getScopes());
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
    @Profile("!basic-auth & !no-auth")
    public JwtDecoder jwtDecoder(AuthConfig authConfig) {
        List<IssuerConfig> issuers = authConfig.getTokenIssuers();

        if (issuers == null || issuers.isEmpty()) {
            throw new IllegalArgumentException("At least one token issuer must be defined");
        }
        issuers = new ArrayList<>(issuers);
        return new DelegatingJwtDecoder(issuers);
    }


    @Bean
    @Profile("!basic-auth & !no-auth")
    public WebSecurityConfigurerAdapter securityConfigurerBearerAuth(){
        return new WebSecurityConfigurerAdapter() {
            @Override
            protected void configure(HttpSecurity http) throws Exception {
                http.authorizeRequests()
                    .antMatchers("/actuator/health", "/actuator/info", "/service-info").permitAll()
                    .antMatchers("/api/**")
                    .authenticated()
                    .and()
                    .oauth2ResourceServer()
                    .jwt()
                    .and()
                    .and()
                    .csrf()
                    .disable();
            }
        };
    }


    @Bean
    @Profile("basic-auth")
    public WebSecurityConfigurerAdapter securityConfigurerDefaultAuth(){
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


    @Bean
    @Profile("no-auth")
    public WebSecurityConfigurerAdapter securityConfigurerNoAuth(){
        return new WebSecurityConfigurerAdapter() {
            @Override
            protected void configure(HttpSecurity http) throws Exception {
                http.authorizeRequests().anyRequest().permitAll().and().csrf().disable();
            }
        };
    }

    private String[] parseCorsUrls() {
        return corsUrls.split(",");
    }
}