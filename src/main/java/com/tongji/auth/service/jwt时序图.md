```mermaid
sequenceDiagram
    participant Client as 客户端
    participant Decode as decode()方法
    participant JwtDecoder as JwtDecoder
    participant Parser as JWT解析器
    participant Validator as 签名验证器
    participant Clock as 系统时钟
    participant KeyStore as 密钥存储
    
    Client->>Decode: 传入JWT字符串
    Decode->>JwtDecoder: decode(token)
    
    JwtDecoder->>Parser: 解析JWT格式
    Parser->>Parser: 分割header.payload.signature
    Parser->>Parser: Base64Url解码header
    Parser->>Parser: Base64Url解码payload
    Parser-->>JwtDecoder: 返回解析结果
    
    JwtDecoder->>Clock: 获取当前时间
    Clock-->>JwtDecoder: 返回当前时间戳
    
    JwtDecoder->>JwtDecoder: 验证exp（过期时间）
    alt 令牌已过期
        JwtDecoder-->>Decode: 抛出JwtException
        Decode-->>Client: 返回错误
    else 令牌未过期
        JwtDecoder->>JwtDecoder: 验证nbf（生效时间）
        alt 令牌未生效
            JwtDecoder-->>Decode: 抛出JwtException
            Decode-->>Client: 返回错误
        else 令牌已生效
            JwtDecoder->>KeyStore: 获取RSA公钥
            KeyStore-->>JwtDecoder: 返回公钥
            
            JwtDecoder->>Validator: 验证签名
            Validator->>Validator: 重新计算签名
            Validator->>Validator: 比较签名
            
            alt 签名不匹配
                Validator-->>JwtDecoder: 验证失败
                JwtDecoder-->>Decode: 抛出JwtException
                Decode-->>Client: 返回错误
            else 签名匹配
                Validator-->>JwtDecoder: 验证成功
                JwtDecoder->>JwtDecoder: 构建Jwt对象
                JwtDecoder-->>Decode: 返回Jwt对象
                Decode-->>Client: 返回Jwt对象
            end
        end
    end
