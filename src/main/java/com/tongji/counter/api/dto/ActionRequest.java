package com.tongji.counter.api.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * 行为请求体：用于点赞/收藏等操作的实体标识。
 */
@Data
public class ActionRequest {
    @NotBlank(message = "实体类型不能为空")
    private String entityType; // 如: knowpost
    @NotBlank(message = "实体ID不能为空")
    private String entityId;   // 内容ID
}