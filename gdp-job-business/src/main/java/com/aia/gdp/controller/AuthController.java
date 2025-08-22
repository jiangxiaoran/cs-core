package com.aia.gdp.controller;

import com.aia.gdp.common.ApiResponse;
import com.aia.gdp.dto.LoginRequest;
import com.aia.gdp.dto.RefreshTokenRequest;
import com.aia.gdp.model.User;
import com.aia.gdp.service.AuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 认证授权控制器
 * 
 * @author andy
 * @date 2025-08-07
 */
@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {
    
    private static final Logger logger = LoggerFactory.getLogger(AuthController.class);
    
    @Autowired
    private AuthService authService;
    
    /**
     * 用户登录
     */
    @PostMapping("/login")
    public ResponseEntity<ApiResponse<Map<String, Object>>> login(@RequestBody LoginRequest request) {
        try {
            logger.info("用户登录请求: username={}", request.getUsername());
            
            Map<String, Object> result = authService.login(request);
            
            return ResponseEntity.ok(ApiResponse.success("登录成功", result));
            
        } catch (Exception e) {
            logger.error("用户登录失败: {}", request.getUsername(), e);
            return ResponseEntity.ok(ApiResponse.error(401, "登录失败: " + e.getMessage()));
        }
    }
    
    /**
     * 用户登出
     */
    @PostMapping("/logout")
    public ResponseEntity<ApiResponse<Object>> logout(@RequestHeader("Authorization") String authorization) {
        try {
            String token = extractToken(authorization);
            logger.info("用户登出请求");
            
            boolean result = authService.logout(token);
            
            if (result) {
                return ResponseEntity.ok(ApiResponse.success("登出成功"));
            } else {
                return ResponseEntity.ok(ApiResponse.error(500, "登出失败"));
            }
            
        } catch (Exception e) {
            logger.error("用户登出失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "登出失败: " + e.getMessage()));
        }
    }
    
    /**
     * 刷新Token
     */
    @PostMapping("/refresh")
    public ResponseEntity<ApiResponse<Map<String, Object>>> refreshToken(@RequestBody RefreshTokenRequest request) {
        try {
            logger.info("Token刷新请求");
            
            Map<String, Object> result = authService.refreshToken(request);
            
            return ResponseEntity.ok(ApiResponse.success("刷新成功", result));
            
        } catch (Exception e) {
            logger.error("Token刷新失败", e);
            return ResponseEntity.ok(ApiResponse.error(401, "Token刷新失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取当前用户信息
     */
    @GetMapping("/current-user")
    public ResponseEntity<ApiResponse<User>> getCurrentUser(@RequestHeader("Authorization") String authorization) {
        try {
            String token = extractToken(authorization);
            logger.info("获取当前用户信息请求");
            
            User user = authService.getCurrentUser(token);
            
            if (user != null) {
                return ResponseEntity.ok(ApiResponse.success("获取成功", user));
            } else {
                return ResponseEntity.ok(ApiResponse.error(401, "用户未登录或Token无效"));
            }
            
        } catch (Exception e) {
            logger.error("获取当前用户信息失败", e);
            return ResponseEntity.ok(ApiResponse.error(401, "获取用户信息失败: " + e.getMessage()));
        }
    }
    
    /**
     * 从Authorization头中提取Token
     */
    private String extractToken(String authorization) {
        if (authorization != null && authorization.startsWith("Bearer ")) {
            return authorization.substring(7);
        }
        return null;
    }
} 