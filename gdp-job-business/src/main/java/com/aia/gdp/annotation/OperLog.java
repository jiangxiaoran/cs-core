package com.aia.gdp.annotation;

import java.lang.annotation.*;

/**
 * 操作日志注解
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OperLog {
    
    /**
     * 模块标题
     */
    String title() default "";
    
    /**
     * 业务类型（0其它 1新增 2修改 3删除）
     */
    int businessType() default 0;
    
    /**
     * 是否保存请求参数
     */
    boolean isSaveRequestData() default true;
    
    /**
     * 是否保存响应数据
     */
    boolean isSaveResponseData() default true;
}