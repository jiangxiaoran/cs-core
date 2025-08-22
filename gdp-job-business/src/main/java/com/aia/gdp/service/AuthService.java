package com.aia.gdp.service;

import com.aia.gdp.dto.LoginRequest;
import com.aia.gdp.dto.RefreshTokenRequest;
import com.aia.gdp.model.User;
import java.util.Map;

/**
 * 认证服务接口
 * 
 * @author andy
 * @date 2025-08-07
 */
public interface AuthService {
    
    /**
     * 用户登录
     */
    Map<String, Object> login(LoginRequest request);
    
    /**
     * 用户登出
     */
    boolean logout(String token);
    
    /**
     * 刷新Token
     */
    Map<String, Object> refreshToken(RefreshTokenRequest request);
    
    /**
     * 获取当前用户信息
     */
    User getCurrentUser(String token);
    
    /**
     * 验证Token
     */
    boolean validateToken(String token);
} 