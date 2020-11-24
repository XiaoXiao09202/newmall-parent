package com.xiao.newmall.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.xiao.newmall.publisher.mapper")
public class NewmallPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(NewmallPublisherApplication.class, args);
    }

}
