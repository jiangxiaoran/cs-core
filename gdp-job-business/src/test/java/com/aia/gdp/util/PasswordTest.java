package com.aia.gdp.util;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

/**
 * 密码加密测试类
 * 用于验证BCrypt密码加密是否正确
 */
public class PasswordTest {
    
    public static void main(String[] args) {
        BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
        
        // 测试密码
        String plainPassword = "zxr_123456";
        
        // 数据库中的加密密码
        String storedPassword = "$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lE9lBOsl7iKTVEFDa";
        
        System.out.println("=== BCrypt密码验证测试 ===");
        System.out.println("明文密码: " + plainPassword);
        System.out.println("存储的加密密码: " + storedPassword);
        
        // 验证密码
        boolean matches = passwordEncoder.matches(plainPassword, storedPassword);
        System.out.println("密码验证结果: " + matches);
        
        // 生成新的加密密码
        String newEncryptedPassword = passwordEncoder.encode(plainPassword);
        System.out.println("新生成的加密密码: " + newEncryptedPassword);
        
        // 验证新生成的密码
        boolean newMatches = passwordEncoder.matches(plainPassword, newEncryptedPassword);
        System.out.println("新密码验证结果: " + newMatches);
        
        // 测试错误的密码
        String wrongPassword = "wrongpassword";
        boolean wrongMatches = passwordEncoder.matches(wrongPassword, storedPassword);
        System.out.println("错误密码验证结果: " + wrongMatches);
        
        // 测试不同的BCrypt强度
        System.out.println("\n=== 测试不同BCrypt强度 ===");
        for (int strength = 10; strength <= 12; strength++) {
            BCryptPasswordEncoder encoder = new BCryptPasswordEncoder(strength);
            String testPassword = encoder.encode(plainPassword);
            boolean testMatches = encoder.matches(plainPassword, testPassword);
            System.out.println("强度 " + strength + ": " + testPassword + " -> " + testMatches);
        }
    }
} 