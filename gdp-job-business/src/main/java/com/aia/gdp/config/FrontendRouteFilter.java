package com.aia.gdp.config;

import org.springframework.stereotype.Component;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 前端路由过滤器
 * 用于处理所有前端路由，确保 SPA 应用能正常工作
 * 
 * @author andy
 * @date 2025-08-22
 */
@Component
public class FrontendRouteFilter implements Filter {
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) 
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String requestURI = httpRequest.getRequestURI();
        
        // 如果请求路径以 /data-platform 开头，且不是静态资源或API，则转发到 index.html
        if (requestURI.startsWith("/data-platform") && 
            !requestURI.contains("/static/") && 
            !requestURI.contains("/api/") &&
            !requestURI.contains(".")) { // 简化判断：只要不包含点号就认为是前端路由
            
            // 转发到 index.html
            request.getRequestDispatcher("/index.html").forward(request, response);
            return; // 不继续处理
        }
        
        chain.doFilter(request, response);
    }
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // 初始化
    }
    
    @Override
    public void destroy() {
        // 销毁
    }
}
