package com.dnastack.ga4gh.search.adapter.config;

import com.dnastack.ga4gh.search.adapter.data.InMemorySearchHistoryService;
import com.dnastack.ga4gh.search.adapter.data.PersistentSearchHistoryService;
import com.dnastack.ga4gh.search.adapter.data.SearchHistoryService;
import com.dnastack.ga4gh.search.adapter.monitoring.Monitor;
import com.dnastack.ga4gh.search.adapter.presto.PagingResultSetConsumerCache;
import com.dnastack.ga4gh.search.adapter.presto.PrestoAdapterImpl;
import com.dnastack.ga4gh.search.adapter.presto.PrestoSearchSource;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig.IssuerConfig;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig.OauthClientConfig;
import com.dnastack.ga4gh.search.adapter.security.DelegatingJwtDecoder;
import com.dnastack.ga4gh.search.adapter.security.ServiceAccountAuthenticator;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;


@Slf4j
@EnableWebSecurity
@Configuration
public class ApplicationConfig {

    @Value("${presto.datasource.url}")
    private String prestoDatasourceUrl;

    @Value("${presto.datasource.username}")
    private String prestoDatasourceUsername;

    @Value("${presto.results.default-page-size}")
    private Integer defaultPageSize;


    @Value("${spring.datasource.url}")
    private String pgUrl;

    @Value("${spring.datasource.username}")
    private String pgUsername;

    @Value("${spring.datasource.password}")
    private String pgPassword;

    /**
     * Other settings
     */
    @Value("${cors.urls}")
    private String corsUrls;

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
    public PrestoSearchSource getPrestoSearchSource(SearchHistoryService searchHistoryService, PagingResultSetConsumerCache consumerCache, ServiceAccountAuthenticator accountAuthenticator, Monitor monitor) {
        return new PrestoSearchSource(searchHistoryService, new PrestoAdapterImpl(prestoDatasourceUrl, accountAuthenticator, monitor), consumerCache);
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


    @Bean
    @Profile("!no-auth")
    public DataSource hakariDataSource() {
        PGSimpleDataSource pgDataSource = new PGSimpleDataSource();
        pgDataSource.setUrl(pgUrl);
        pgDataSource.setUser(pgUsername);
        pgDataSource.setPassword(pgPassword);
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setMaximumPoolSize(6);
        hikariConfig.setDataSource(pgDataSource);
        return new HikariDataSource(hikariConfig);
    }

    @Bean
    @Profile("!no-auth")
    public Jdbi jdbi(DataSource dataSource) {
        return Jdbi.create(dataSource)
            .installPlugin(new SqlObjectPlugin());
    }

    @Bean
    @Profile("!no-auth")
    public SearchHistoryService persistentSearchHistory(Jdbi jdbi) {
        log.info("Using persistent query storage");
        return new PersistentSearchHistoryService(jdbi);
    }

    @Bean
    @Profile("no-auth")
    public SearchHistoryService inMemorySearchHistory() {
        log.info("Using in memory query storage. Only keeping at most 10 queries");
        return new InMemorySearchHistoryService(10);
    }

    private String[] parseCorsUrls() {
        return corsUrls.split(",");
    }
}
