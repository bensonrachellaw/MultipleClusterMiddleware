package com.szubd.mcdfs_and_rspc;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration //声明该类为配置类
@EnableSwagger2 //声明启动Swagger2

public class SwaggerConfig {
    @Bean
    public Docket customDocket() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.szubd.mcdfs_and_rspc.controller"))//扫描的包路径
                .build();
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("跨域协同计算中央控制中心")//文档说明
                .version("2.0.0")//文档版本说明
                .description("Copyright belongs to Amo")
                .license("Apache 2.0许可")
                .build();
    }
}
