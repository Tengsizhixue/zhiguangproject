```mermaid
sequenceDiagram
    participant Client as 客户端
    participant Filter as JwtAuthenticationFilter
    participant Decoder as JwtDecoder
    participant Parser as JWT解析器
    participant Validator as 签名验证器
    participant KeyStore as 密钥存储
    participant UserDetailsService as 用户详情服务
    participant SecurityContext as SecurityContext
    participant Controller as 业务Controller
    
    Client->>Filter: GET /api/user/profile<br/>Authorization: Bearer {token}
    
    Filter->>Filter: 提取Bearer Token
    Filter->>Decoder: decode(token)
    
    Decoder->>Parser: 解析JWT格式
    Parser->>Parser: 分割header.payload.signature
    Parser->>Parser: Base64Url解码header
    Parser->>Parser: Base64Url解码payload
    Parser-->>Decoder: 返回解析结果
    
    Decoder->>Decoder: 验证算法类型
    alt 算法不支持
        Decoder-->>Filter: 抛出JwtException
        Filter-->>Client: 返回401 Unauthorized
    end
    
    Decoder->>Decoder: 验证时间声明
    Decoder->>Decoder: 检查exp（过期时间）
    alt 令牌已过期
        Decoder-->>Filter: 抛出JwtException
        Filter-->>Client: 返回401 Unauthorized
    end
    
    Decoder->>Decoder: 检查nbf（生效时间）
    alt 令牌未生效
        Decoder-->>Filter: 抛出JwtException
        Filter-->>Client: 返回401 Unauthorized
    end
    
    Decoder->>KeyStore: 获取RSA公钥
    KeyStore-->>Decoder: 返回公钥
    
    Decoder->>Validator: 验证签名
    Validator->>Validator: 重建签名数据
    Validator->>Validator: 计算签名
    Validator->>Validator: 比较签名
    
    alt 签名不匹配
        Validator-->>Decoder: 验证失败
        Decoder-->>Filter: 抛出JwtException
        Filter-->>Client: 返回401 Unauthorized
    end
    
    Validator-->>Decoder: 验证成功
    Decoder->>Decoder: 构建Jwt对象
    Decoder-->>Filter: 返回Jwt对象
    
    Filter->>Filter: 提取用户ID
    Filter->>UserDetailsService: loadUserByUsername(userId)
    
    UserDetailsService->>UserDetailsService: 查询数据库
    alt 用户不存在或禁用
        UserDetailsService-->>Filter: 抛出异常
        Filter-->>Client: 返回401 Unauthorized
    end
    
    UserDetailsService-->>Filter: 返回UserDetails
    Filter->>Filter: 构建认证对象
    Note over Filter: UsernamePasswordAuthenticationToken<br/>principal=userDetails<br/>credentials=null<br/>authorities=roles
    
    Filter->>SecurityContext: setAuthentication(auth)
    SecurityContext->>SecurityContext: 存储认证信息
    
    Filter->>Controller: 继续过滤器链
    Controller->>Controller: 执行业务逻辑
    Note over Controller: 可通过SecurityContext<br/>获取当前用户信息
    
    Controller-->>Client: 返回业务数据
    
    SecurityContext->>SecurityContext: 清理认证信息<br/>请求结束后