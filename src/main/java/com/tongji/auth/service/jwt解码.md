```mermaid
flowchart TD
    Start([开始: 接收JWT令牌字符串]) --> Parse[解析JWT格式]
    Parse --> Split{分割三部分}
    Split -->|成功| Header[解析Header]
    Split -->|失败| Error1[抛出JwtException<br/>JWT格式错误]
    
    Header --> ExtractAlg[提取算法信息]
    ExtractAlg --> CheckAlg{检查算法}
    CheckAlg -->|RS256| Payload[解析Payload]
    CheckAlg -->|其他算法| Error2[抛出JwtException<br/>不支持的算法]
    
    Payload --> Base64Decode[Base64Url解码]
    Base64Decode --> ParseClaims[解析Claims声明]
    ParseClaims --> ValidateTime[验证时间声明]
    
    ValidateTime --> CheckExp{检查过期时间exp}
    CheckExp -->|已过期| Error3[抛出JwtException<br/>令牌已过期]
    CheckExp -->|未过期| CheckNbf{检查生效时间nbf}
    
    CheckNbf -->|未生效| Error4[抛出JwtException<br/>令牌未生效]
    CheckNbf -->|已生效| CheckIat{检查签发时间iat}
    
    CheckIat -->|异常| Error5[抛出JwtException<br/>签发时间异常]
    CheckIat -->|正常| ReadKey[读取RSA公钥]
    
    ReadKey --> BuildSignature[重新构建签名]
    BuildSignature --> VerifySig{验证签名}
    
    VerifySig -->|签名不匹配| Error6[抛出JwtException<br/>签名验证失败]
    VerifySig -->|签名匹配| BuildJwt[构建Jwt对象]
    
    BuildJwt --> ExtractClaims[提取所有声明]
    ExtractClaims --> SetHeaders[设置头信息]
    SetHeaders --> End([返回Jwt对象])
    
    Error1 --> EndError([抛出异常])
    Error2 --> EndError
    Error3 --> EndError
    Error4 --> EndError
    Error5 --> EndError
    Error6 --> EndError
