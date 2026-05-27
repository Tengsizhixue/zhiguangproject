package com.tongji;


import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.net.UnknownHostException;

@SpringBootApplication
@ConfigurationPropertiesScan(basePackages = "com.tongji.config")
@Slf4j
@MapperScan(basePackages = "com.tongji.*.mapper")
public class ZhiGuangApplication {
    public static void main(String[] args) throws UnknownHostException {
        // 获取 Spring 上下文
        ConfigurableApplicationContext application = SpringApplication.run(ZhiGuangApplication.class, args);
        // 获取环境变量
        Environment env = application.getEnvironment();
        String ip = InetAddress.getLocalHost().getHostAddress();
        String port = env.getProperty("server.port" , "8080");
        // 防止未配置 context-path 时报错，默认为空字符串
        String path = env.getProperty("server.servlet.context-path");
        if (path == null) {
            path = "";
        }

        // 拼接并打印炫酷的启动日志
        log.info("\n----------------------------------------------------------\n\t" +
                "知光系统 (Zhiguang) 启动成功! 🚀\n\t" +
                "----------------------------------------------------------\n\t" +
                "Local: \t\thttp://localhost/" +"\n\t" +
                "External: \thttp://" + ip + ":" + port + path + "/\n\t" +
                "Doc文档: \thttp://" + ip + ":" + port + path + "/doc.html\n\t" + // 如果你用的是 Swagger 原生，改成 /swagger-ui.html
                "----------------------------------------------------------\n\t" +
                "当前环境: \t" + env.getProperty("spring.profiles.active") + "\n" +
                "----------------------------------------------------------");
    }

}
