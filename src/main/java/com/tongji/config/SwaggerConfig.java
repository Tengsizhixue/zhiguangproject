package com.tongji.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                // 1. 设置 API 基础信息
                .info(new Info()
                        .title("知光项目 API 接口文档")
                        .version("v1.0.0")
                        .description("基于 Spring Boot 3 和 Spring Security 打造的 API 接口集"))
                // 2. 告诉 Swagger 我们所有的接口都需要叫做 "BearerAuth" 的安全验证
                .addSecurityItem(new SecurityRequirement().addList("BearerAuth"))
                // 3. 定义 "BearerAuth" 到底是个什么东西（是一个放 JWT 的 HTTP Bearer 头）
                .components(new Components()
                        .addSecuritySchemes("BearerAuth", new SecurityScheme()
                                .name("BearerAuth")
                                .type(SecurityScheme.Type.HTTP)
                                .scheme("bearer")
                                .bearerFormat("JWT")));
    }
}