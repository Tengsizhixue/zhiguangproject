```mermaid
flowchart TD
    Start([客户端请求<br/>携带JWT令牌]) --> Extract[JwtAuthenticationFilter<br/>提取令牌]
    
    Extract --> HasToken{请求头包含<br/>Authorization?}
    HasToken -->|否| NoToken[继续过滤器链<br/>未认证状态]
    HasToken -->|是| ExtractToken[提取Bearer Token]
    
    ExtractToken --> ValidateFormat{令牌格式<br/>是否正确?}
    ValidateFormat -->|否| FormatError[抛出JwtException<br/>格式错误]
    ValidateFormat -->|是| SplitToken[分割JWT<br/>header.payload.signature]
    
    SplitToken --> DecodeHeader[Base64Url解码<br/>Header]
    DecodeHeader --> ParseHeader[解析Header JSON<br/>提取算法信息]
    
    ParseHeader --> CheckAlgorithm{算法检查<br/>是否为RS256?}
    CheckAlgorithm -->|否| AlgoError[抛出JwtException<br/>不支持的算法]
    CheckAlgorithm -->|是| DecodePayload[Base64Url解码<br/>Payload]
    
    DecodePayload --> ParsePayload[解析Payload JSON<br/>提取Claims]
    ParsePayload --> ValidateTime[时间验证阶段]
    
    ValidateTime --> CheckExp{检查exp<br/>过期时间}
    CheckExp -->|已过期| ExpError[抛出JwtException<br/>令牌已过期]
    CheckExp -->|未过期| CheckNbf{检查nbf<br/>生效时间}
    
    CheckNbf -->|未生效| NbfError[抛出JwtException<br/>令牌未生效]
    CheckNbf -->|已生效| CheckIat{检查iat<br/>签发时间}
    
    CheckIat -->|异常| IatError[抛出JwtException<br/>签发时间异常]
    CheckIat -->|正常| VerifySignature[签名验证阶段]
    
    VerifySignature --> ReadPublicKey[读取RSA公钥<br/>从配置文件]
    ReadPublicKey --> RebuildData[重建签名数据<br/>header.payload]
    
    RebuildData --> CalcSignature[使用公钥验证签名<br/>SHA256withRSA]
    CalcSignature --> CompareSig{签名比较<br/>计算值==存储值?}
    
    CompareSig -->|不匹配| SigError[抛出JwtException<br/>签名验证失败]
    CompareSig -->|匹配| ExtractClaims[提取Claims声明]
    
    ExtractClaims --> BuildJwt[构建Jwt对象<br/>封装所有信息]
    BuildJwt --> ExtractUserId[提取用户ID<br/>从sub或user_id声明]
    
    ExtractUserId --> LoadUserDetails[加载用户详情<br/>UserDetailsService]
    LoadUserDetails --> CheckUser{用户存在且<br/>状态正常?}
    
    CheckUser -->|否| UserError[抛出异常<br/>用户不存在或禁用]
    CheckUser -->|是| BuildAuth[构建认证对象<br/>UsernamePasswordAuthenticationToken]
    
    BuildAuth --> SetContext[设置安全上下文<br/>SecurityContextHolder]
    SetContext --> Continue[继续过滤器链<br/>已认证状态]
    
    Continue --> Business[业务方法执行<br/>可访问用户信息]
    Business --> Response[返回响应]
    
    NoToken --> Response
    FormatError --> ErrorResponse([返回401错误])
    AlgoError --> ErrorResponse
    ExpError --> ErrorResponse
    NbfError --> ErrorResponse
    IatError --> ErrorResponse
    SigError --> ErrorResponse
    UserError --> ErrorResponse

