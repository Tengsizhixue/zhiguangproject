package com.tongji.auth.mapper;

import com.tongji.auth.audit.LoginLog;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface LoginLogMapper {

    void insert(LoginLog log);
}