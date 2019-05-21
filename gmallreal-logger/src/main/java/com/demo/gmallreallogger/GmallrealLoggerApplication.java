package com.demo.gmallreallogger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.demo.logger","com.demo.test.hello"})
public class GmallrealLoggerApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallrealLoggerApplication.class, args);
    }

}
