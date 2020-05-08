package com.dnastack.ga4gh.search;

import com.dnastack.ga4gh.search.adapter.presto.PrestoClient;
import com.dnastack.ga4gh.search.adapter.presto.PrestoHttpClient;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig.IssuerConfig;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig.OauthClientConfig;
import com.dnastack.ga4gh.search.adapter.security.DelegatingJwtDecoder;
import com.dnastack.ga4gh.search.adapter.security.ServiceAccountAuthenticator;
import com.dnastack.ga4gh.search.adapter.telemetry.Monitor;
import com.dnastack.ga4gh.search.adapter.telemetry.PrestoTelemetryClient;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;


@Slf4j
@EnableWebSecurity
@Configuration
public class ApplicationConfig {

    @Value("${presto.datasource.url}")
    private String prestoDatasourceUrl;

    /**
     * Other settings
     */
    @Value("${cors.urls}")
    private String corsUrls;

    @Autowired
    private Monitor monitor;

    @Autowired
    private Converter<Jwt, ? extends AbstractAuthenticationToken> jwtScopesConverter;

    @Bean
    public ServiceAccountAuthenticator getServiceAccountAuthenticator(AuthConfig authConfig) {
        OauthClientConfig clientConfig = authConfig.getPrestoOauthClient();
        if (clientConfig == null) {
            return new ServiceAccountAuthenticator();
        } else {
            return new ServiceAccountAuthenticator(clientConfig);
        }
    }

    @Bean
    public PrestoClient getPrestoClient(ServiceAccountAuthenticator accountAuthenticator) {
        return new PrestoTelemetryClient(new PrestoHttpClient(prestoDatasourceUrl, accountAuthenticator));
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
    @Profile("default")
    public JwtDecoder jwtDecoder(AuthConfig authConfig) {
        List<IssuerConfig> issuers = authConfig.getTokenIssuers();

        if (issuers == null || issuers.isEmpty()) {
            throw new IllegalArgumentException("At least one token issuer must be defined");
        }
        issuers = new ArrayList<>(issuers);
        return new DelegatingJwtDecoder(issuers);
    }


    @Bean
    @Profile("default")
    public WebSecurityConfigurerAdapter securityConfigurerBearerAuth() {
        return new WebSecurityConfigurerAdapter() {
            @Override
            protected void configure(HttpSecurity http) throws Exception {
                http.authorizeRequests()
                    .antMatchers("/actuator/health", "/actuator/info", "/service-info").permitAll()
                    .antMatchers("/**")
                    .authenticated()
                    .and()
                    .oauth2ResourceServer()
                    .jwt().jwtAuthenticationConverter(jwtScopesConverter)
                    .and()
                    .and()
                    .csrf()
                    .disable();
            }
        };
    }


    @Bean
    @Profile("basic-auth")
    public WebSecurityConfigurerAdapter securityConfigurerDefaultAuth() {
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
    public WebSecurityConfigurerAdapter securityConfigurerNoAuth() {
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
