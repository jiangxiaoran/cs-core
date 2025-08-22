package com.aia.gdp.service.impl;

import com.aia.gdp.dto.LoginRequest;
import com.aia.gdp.dto.RefreshTokenRequest;
import com.aia.gdp.mapper.UserMapper;
import com.aia.gdp.model.User;
import com.aia.gdp.service.AuthService;
import com.aia.gdp.common.JwtUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * 认证服务实现类
 * 
 * @author andy
 * @date 2025-08-05
 */
@Service
public class AuthServiceImpl implements AuthService {
    
    private static final Logger logger = LoggerFactory.getLogger(AuthServiceImpl.class);
    
    @Autowired
    private UserMapper userMapper;
    
    @Autowired
    private JwtUtil jwtUtil;
    
    private final BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
    
    // 存储已登出的Token（实际项目中应该使用Redis）
    private final Set<String> blacklistedTokens = new HashSet<>();
    
    @Override
    public Map<String, Object> login(LoginRequest request) {
        try {
            // 验证用户名和密码
            if (!StringUtils.hasText(request.getUsername()) || !StringUtils.hasText(request.getPassword())) {
                throw new RuntimeException("用户名和密码不能为空");
            }
            
            // 查询用户
            User user = userMapper.selectByUsername(request.getUsername());
            if (user == null) {
                throw new RuntimeException("用户不存在");
            }
            
            // 添加调试日志
            logger.info("用户登录尝试: username={}, storedPassword={}", request.getUsername(), user.getPassword());
            
            // 验证密码
            boolean passwordMatches = passwordEncoder.matches(request.getPassword(), user.getPassword());
            logger.info("密码验证结果: {}", passwordMatches);
            
            if (!passwordMatches) {
                throw new RuntimeException("密码错误");
            }
            
            // 检查用户状态
            if (!user.getIsActive()) {
                throw new RuntimeException("用户已被禁用");
            }
            
            // 生成Token
            String token = jwtUtil.generateToken(user.getUsername(), user.getUserId());
            String refreshToken = jwtUtil.generateRefreshToken(user.getUsername(), user.getUserId());
            
            // 更新最后登录时间
            user.setLastLoginTime(new Date());
            userMapper.updateById(user);
            
            // 处理权限列表
            List<String> permissions = parsePermissions(user.getPermissions());
            user.setPermissionList(permissions);
            
            // 构建返回数据
            Map<String, Object> result = new HashMap<>();
            result.put("token", token);
            result.put("refreshToken", refreshToken);
            result.put("expiresIn", 7200);
            result.put("user", user);
            
            logger.info("用户登录成功: {}", user.getUsername());
            return result;
            
        } catch (Exception e) {
            logger.error("用户登录失败: {}", request.getUsername(), e);
            throw new RuntimeException("登录失败: " + e.getMessage());
        }
    }
    
    @Override
    public boolean logout(String token) {
        try {
            if (StringUtils.hasText(token)) {
                // 将Token加入黑名单
                blacklistedTokens.add(token);
                logger.info("用户登出成功");
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.error("用户登出失败", e);
            return false;
        }
    }
    
    @Override
    public Map<String, Object> refreshToken(RefreshTokenRequest request) {
        try {
            if (!StringUtils.hasText(request.getRefreshToken())) {
                throw new RuntimeException("刷新Token不能为空");
            }
            
            // 验证刷新Token
            if (!jwtUtil.isRefreshToken(request.getRefreshToken())) {
                throw new RuntimeException("无效的刷新Token");
            }
            
            if (jwtUtil.isTokenExpired(request.getRefreshToken())) {
                throw new RuntimeException("刷新Token已过期");
            }
            
            // 从刷新Token中获取用户信息
            String username = jwtUtil.getUsernameFromToken(request.getRefreshToken());
            Long userId = jwtUtil.getUserIdFromToken(request.getRefreshToken());
            
            // 查询用户
            User user = userMapper.selectByUsername(username);
            if (user == null) {
                throw new RuntimeException("用户不存在");
            }
            
            // 生成新的Token
            String newToken = jwtUtil.generateToken(username, userId);
            String newRefreshToken = jwtUtil.generateRefreshToken(username, userId);
            
            // 构建返回数据
            Map<String, Object> result = new HashMap<>();
            result.put("token", newToken);
            result.put("refreshToken", newRefreshToken);
            result.put("expiresIn", 7200);
            
            logger.info("Token刷新成功: {}", username);
            return result;
            
        } catch (Exception e) {
            logger.error("Token刷新失败", e);
            throw new RuntimeException("Token刷新失败: " + e.getMessage());
        }
    }
    
    @Override
    public User getCurrentUser(String token) {
        try {
            if (!StringUtils.hasText(token)) {
                return null;
            }
            
            // 检查Token是否在黑名单中
            if (blacklistedTokens.contains(token)) {
                return null;
            }
            
            // 验证Token
            if (!validateToken(token)) {
                return null;
            }
            
            // 从Token中获取用户信息
            String username = jwtUtil.getUsernameFromToken(token);
            User user = userMapper.selectByUsername(username);
            
            if (user != null) {
                // 处理权限列表
                List<String> permissions = parsePermissions(user.getPermissions());
                user.setPermissionList(permissions);
            }
            
            return user;
            
        } catch (Exception e) {
            logger.error("获取当前用户信息失败", e);
            return null;
        }
    }
    
    @Override
    public boolean validateToken(String token) {
        try {
            if (!StringUtils.hasText(token)) {
                return false;
            }
            
            // 检查Token是否在黑名单中
            if (blacklistedTokens.contains(token)) {
                return false;
            }
            
            // 验证Token格式和签名
            String username = jwtUtil.getUsernameFromToken(token);
            return jwtUtil.validateToken(token, username);
            
        } catch (Exception e) {
            logger.error("Token验证失败", e);
            return false;
        }
    }
    
    /**
     * 解析权限字符串为列表
     */
    private List<String> parsePermissions(String permissions) {
        List<String> permissionList = new ArrayList<>();
        if (StringUtils.hasText(permissions)) {
            String[] perms = permissions.split(",");
            for (String perm : perms) {
                if (StringUtils.hasText(perm.trim())) {
                    permissionList.add(perm.trim());
                }
            }
        }
        return permissionList;
    }
} 