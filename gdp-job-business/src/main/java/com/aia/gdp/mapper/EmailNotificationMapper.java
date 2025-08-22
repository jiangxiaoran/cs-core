package com.aia.gdp.mapper;

import com.aia.gdp.model.EmailNotification;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import java.util.List;

@Mapper
public interface EmailNotificationMapper extends BaseMapper<EmailNotification> {
    List<EmailNotification> selectByJobCode(@Param("jobCode") String jobCode);
    List<EmailNotification> selectAll();
    EmailNotification selectById(@Param("id") Long id);
    int insert(EmailNotification notification);
    int update(EmailNotification notification);
    int deleteById(@Param("id") Long id);
} 