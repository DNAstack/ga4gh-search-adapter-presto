package com.dnastack.ga4gh.search.adapter.config;

import io.prestosql.sql.tree.Query;
import java.util.ArrayList;
import java.util.List;
import com.dnastack.ga4gh.search.adapter.model.serde.QueryDeserializer;
import com.dnastack.ga4gh.search.adapter.model.source.SearchSource;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig.IssuerConfig;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig.OauthClientConfig;
import com.dnastack.ga4gh.search.adapter.security.DelegatingJwtDecoder;
import com.dnastack.ga4gh.search.adapter.presto.PrestoAdapterImpl;
import com.dnastack.ga4gh.search.adapter.presto.PrestoSearchSource;
import com.dnastack.ga4gh.search.adapter.auth.ServiceAccountAuthenticator;
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

    @Value("${presto.results.default-page-size}")
    private Integer defaultPageSize;


    /**
     * Other settings
     */
    @Value("${cors.urls}")
    private String corsUrls;

    @Bean
    public ServiceAccountAuthenticator getServiceAccountAuthenticator(AuthConfig authConfig) {
        OauthClientConfig clientConfig = authConfig.getPrestoOauthClient();
        return new ServiceAccountAuthenticator(clientConfig);
    }

    @Bean
    public SearchSource getPrestoSearchSource(ServiceAccountAuthenticator accountAuthenticator) {
        return new PrestoSearchSource(new PrestoAdapterImpl(prestoDatasourceUrl, prestoDatasourceUsername, accountAuthenticator),defaultPageSize);
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
