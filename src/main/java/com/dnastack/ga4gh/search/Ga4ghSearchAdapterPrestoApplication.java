package com.dnastack.ga4gh.search;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class Ga4ghSearchAdapterPrestoApplication {

    public static void main(String[] args) {
        SpringApplication.run(Ga4ghSearchAdapterPrestoApplication.class, args);
    }
}
