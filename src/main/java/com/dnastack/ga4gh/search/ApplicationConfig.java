package com.dnastack.ga4gh.search;

import com.dnastack.auth.JwtTokenParser;
import com.dnastack.auth.JwtTokenParserFactory;
import com.dnastack.auth.PermissionChecker;
import com.dnastack.auth.PermissionCheckerFactory;
import com.dnastack.auth.keyresolver.CachingIssuerPubKeyJwksResolver;
import com.dnastack.auth.keyresolver.IssuerPubKeyStaticResolver;
import com.dnastack.auth.model.IssuerInfo;
import com.dnastack.ga4gh.search.adapter.presto.PrestoClient;
import com.dnastack.ga4gh.search.adapter.presto.PrestoHttpClient;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig.OauthClientConfig;
import com.dnastack.ga4gh.search.adapter.security.DelegatingJwtDecoder;
import com.dnastack.ga4gh.search.adapter.security.ServiceAccountAuthenticator;
import com.dnastack.ga4gh.search.adapter.telemetry.Monitor;
import com.dnastack.ga4gh.search.adapter.telemetry.PrestoTelemetryClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwsHeader;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

    @Value("${presto.hidden-catalogs}")
    @Getter
    private Set<String> hiddenCatalogs;

    /**
     * Other settings
     */
    @Value("${cors.urls}")
    private String corsUrls;

    private final Monitor monitor;

    private final Converter<Jwt, ? extends AbstractAuthenticationToken> jwtScopesConverter;

    @Autowired
    public ApplicationConfig(Monitor monitor, Converter<Jwt, ? extends AbstractAuthenticationToken> jwtScopesConverter) {
        this.monitor = monitor;
        this.jwtScopesConverter = jwtScopesConverter;
    }

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

    @ConditionalOnExpression("'${app.auth.authorization-type}' == 'bearer' && '${app.auth.access-evaluator}' == 'scope'")
    @Configuration
    protected static class DefaultJwtSecurityConfig extends WebSecurityConfigurerAdapter {
        private final Converter<Jwt, ? extends AbstractAuthenticationToken> jwtScopesConverter;

        @Autowired
        public DefaultJwtSecurityConfig(Converter<Jwt, ? extends AbstractAuthenticationToken> jwtScopesConverter) {
            this.jwtScopesConverter = jwtScopesConverter;
        }

        @Override
        protected void configure(HttpSecurity http) throws Exception {
            http.cors().and()
                    .authorizeRequests()
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

        @Bean
        public JwtDecoder jwtDecoderDefault(AuthConfig authConfig) {
            List<AuthConfig.IssuerConfig> issuers = authConfig.getTokenIssuers();

            if (issuers == null || issuers.isEmpty()) {
                throw new IllegalArgumentException("At least one token issuer must be defined");
            }
            issuers = new ArrayList<>(issuers);
            return new DelegatingJwtDecoder(issuers);
        }
    }

    @ConditionalOnClass(PermissionChecker.class)
    @ConditionalOnExpression("'${app.auth.authorization-type}' == 'bearer' && '${app.auth.access-evaluator}' == 'wallet'")
    @Configuration
    protected static class WalletJwtSecurityConfig extends WebSecurityConfigurerAdapter {
        @Override
        protected void configure(HttpSecurity http) throws Exception {
            http.cors().and()
                    .authorizeRequests()
                    .antMatchers("/actuator/health", "/actuator/info", "/service-info").permitAll()
                    .antMatchers("/**")
                    .authenticated()
                    .and()
                    .oauth2ResourceServer()
                    .jwt()
                    .and()
                    .and()
                    .csrf()
                    .disable();
        }

        @Bean
        public List<IssuerInfo> allowedIssuers(AuthConfig authConfig) {
            List<AuthConfig.IssuerConfig> issuers = authConfig.getTokenIssuers();
            if (issuers == null || issuers.isEmpty()) {
                throw new IllegalArgumentException("At least one token issuer must be defined");
            }

            return authConfig.getTokenIssuers().stream()
                    .map((issuerConfig) -> {
                        final String issuerUri = issuerConfig.getIssuerUri();
                        return IssuerInfo.IssuerInfoBuilder.builder()
                                .issuerUri(issuerUri)
                                .allowedAudiences(issuerConfig.getAudiences())
                                .publicKeyResolver(issuerConfig.getRsaPublicKey() != null
                                        ? new IssuerPubKeyStaticResolver(issuerUri, issuerConfig.getRsaPublicKey())
                                        : new CachingIssuerPubKeyJwksResolver(issuerUri))
                                .build();
                    })
                    .collect(Collectors.toUnmodifiableList());
        }

        @Bean
        public PermissionChecker permissionChecker(List<IssuerInfo> allowedIssuers) {
            return PermissionCheckerFactory.create(allowedIssuers);
        }

        @Bean
        public JwtDecoder jwtDecoderWallet(List<IssuerInfo> allowedIssuers, PermissionChecker permissionChecker) {
            return (jwtToken) -> {
                permissionChecker.checkPermissions(jwtToken);
                final JwtTokenParser jwtTokenParser = JwtTokenParserFactory.create(allowedIssuers);
                final Jws<Claims> jws = jwtTokenParser.parseTokenClaims(jwtToken);
                final JwsHeader headers = jws.getHeader();
                final Claims claims = jws.getBody();
                return new Jwt(jwtToken, claims.getIssuedAt().toInstant(), claims.getExpiration().toInstant(), headers, claims);
            };
        }
    }

    @ConditionalOnExpression("'${app.auth.authorization-type}' == 'basic'")
    @Configuration
    protected static class BasicAuthSecurityConfig extends WebSecurityConfigurerAdapter {
        @Override
        protected void configure(HttpSecurity http) throws Exception {
            http.cors().and()
                    .authorizeRequests()
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
    }

    @ConditionalOnExpression("'${app.auth.authorization-type}' == 'none'")
    @Configuration
    protected static class NoAuthSecurityConfig extends WebSecurityConfigurerAdapter {
        @Override
        protected void configure(HttpSecurity http) throws Exception {
            http.cors().and().authorizeRequests().anyRequest().permitAll().and().csrf().disable();
        }
    }

    private String[] parseCorsUrls() {
        return corsUrls.split(",");
    }
}
