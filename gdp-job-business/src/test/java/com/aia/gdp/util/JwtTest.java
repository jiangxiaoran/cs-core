package com.aia.gdp.util;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * JWT测试类
 * 用于验证JWT token生成是否正常
 */
public class JwtTest {
    
    public static void main(String[] args) {
        String secret = "gdpJobSecretKey2024SecureAndStrongForHS512AlgorithmWithAtLeast512BitsKeySize";
        long expiration = 7200;
        
        System.out.println("=== JWT Token生成测试 ===");
        System.out.println("Secret: " + secret);
        System.out.println("Secret length: " + secret.length() + " characters");
        System.out.println("Expiration: " + expiration + " seconds");
        
        // 测试token生成
        try {
            String token = createToken("admin", 1L, secret, expiration);
            System.out.println("生成的Token: " + token);
            
            // 验证token
            SecretKey signingKey = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
            Claims claims = Jwts.parserBuilder()
                    .setSigningKey(signingKey)
                    .build()
                    .parseClaimsJws(token)
                    .getBody();
            System.out.println("Token验证成功");
            System.out.println("用户名: " + claims.getSubject());
            System.out.println("用户ID: " + claims.get("userId"));
            System.out.println("过期时间: " + claims.getExpiration());
            
        } catch (Exception e) {
            System.err.println("JWT Token生成失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static String createToken(String username, Long userId, String secret, long expiration) {
        Map<String, Object> claims = new HashMap<>();
        claims.put("username", username);
        claims.put("userId", userId);
        
        SecretKey signingKey = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        
        return Jwts.builder()
                .setClaims(claims)
                .setSubject(username)
                .setIssuedAt(new Date(System.currentTimeMillis()))
                .setExpiration(new Date(System.currentTimeMillis() + expiration * 1000))
                .signWith(signingKey, SignatureAlgorithm.HS512)
                .compact();
    }
} 