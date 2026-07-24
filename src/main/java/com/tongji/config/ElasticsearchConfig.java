package com.tongji.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.RequiredArgsConstructor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * Elasticsearch 客户端配置类。
 * <p>
 * 职责：创建并注册 {@link ElasticsearchClient} Bean，供搜索、RAG 向量存储等模块使用。
 * 认证策略：通过 {@link EsProperties} 读取配置，若配置了用户名密码则启用 Basic Auth，否则无认证连接。
 * 依赖：EsProperties（ES 连接参数）、JacksonJsonpMapper（JSON 序列化）。
 */
@Configuration
@EnableConfigurationProperties(EsProperties.class)
@RequiredArgsConstructor
public class ElasticsearchConfig {

    /** ES 连接配置属性，包含 uris、username、password 等信息 */
    private final EsProperties props;

    /**
     * 创建 Elasticsearch 客户端 Bean。
     * <p>
     * 使用新版 ES Java Client（8.x），底层通过 RestClient 通信，
     * 序列化层使用 JacksonJsonpMapper 与项目 JSON 体系保持一致。
     *
     * @return 配置好的 ElasticsearchClient 实例，可直接注入到 Service 中使用
     */
    @Bean
    public ElasticsearchClient elasticsearchClient() {
        // 1. 创建凭证提供器：用于 HTTP Basic Auth 认证
        //    - 默认不设置任何凭证，适用于 ES 未开启安全认证的场景
        BasicCredentialsProvider creds = new BasicCredentialsProvider();

        // 2. 条件认证：如果配置中提供了用户名，则启用 Basic Auth
        //    - 使用 AuthScope.ANY 表示对所有请求都携带凭证
        //    - 用户名和密码来自 application.yml 中的 spring.elasticsearch.* 配置
        //    - 当前环境 ES 关闭了 xpack.security，username 为空，不进入此分支
        if (StringUtils.hasText(props.getUsername())) {
            creds.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(props.getUsername(), props.getPassword()));
        }

        // 3. 构建 RestClient：低层 HTTP 客户端，负责与 ES 集群通信
        //    - 通过 HttpHost.create() 解析 ES 地址（如 http://localhost:9200 ）
        //    - 将凭证提供器注入到 HTTP 客户端回调中，实现自动认证
        RestClientBuilder builder = RestClient.builder(org.apache.http.HttpHost.create(props.getHost()))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultCredentialsProvider(creds));

        // 4. 创建 RestClient 实例：基于上一步的 Builder 构建
        RestClient restClient = builder.build();

        // 5. 创建传输层：将 RestClient 包装为 ES 官方传输层
        //    - JacksonJsonpMapper：使用 Jackson 作为 JSON 序列化引擎
        //    - 与项目中使用的 ObjectMapper 体系一致，减少序列化兼容问题
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        // 6. 创建最终客户端：将传输层注入，返回可直接使用的 ElasticsearchClient
        //    - 该 Bean 会被 Spring 管理，可通过 @Autowired 注入到任何 Service 中
        //    - 支持索引 CRUD、文档搜索、聚合分析等所有 ES 操作
        return new ElasticsearchClient(transport);
    }
}