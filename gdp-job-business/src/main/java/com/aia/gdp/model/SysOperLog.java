package com.aia.gdp.model;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 操作日志记录
 */
@Data
@EqualsAndHashCode(callSuper = false)
@TableName("sys_oper_log")
public class SysOperLog {

    /**
     * 日志主键
     */
    @TableId(value = "oper_id", type = IdType.AUTO)
    private Long operId;

    /**
     * 模块标题
     */
    private String title;

    /**
     * 业务类型（0其它 1新增 2修改 3删除）
     */
    private Integer businessType;

    /**
     * 方法名称
     */
    private String method;

    /**
     * 请求方式
     */
    private String requestMethod;

    /**
     * 操作类别（0其它 1后台用户 2手机端用户）
     */
    private Integer operatorType;

    /**
     * 操作人员
     */
    private String operName;

    /**
     * 部门名称
     */
    private String deptName;

    /**
     * 请求URL
     */
    private String operUrl;

    /**
     * 主机地址
     */
    private String operIp;

    /**
     * 操作地点
     */
    private String operLocation;

    /**
     * 请求参数
     */
    private String operParam;

    /**
     * 返回参数
     */
    private String jsonResult;

    /**
     * 操作状态（0正常 1异常）
     */
    private Integer status;

    /**
     * 错误消息
     */
    private String errorMsg;

    /**
     * 操作时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime operTime;

    /**
     * 消耗时间
     */
    private Long costTime;

    /**
     * 业务类型常量
     */
    public static final class BusinessType {
        // 基础操作类型
        public static final int OTHER = 0;      // 其它/查询
        public static final int INSERT = 1;     // 新增/创建
        public static final int UPDATE = 2;     // 修改/更新
        public static final int DELETE = 3;     // 删除/移除
        
        // 任务调度相关
        public static final int EXECUTE = 4;    // 执行/运行任务
        public static final int SCHEDULE = 5;   // 调度/计划任务
        public static final int PAUSE = 6;      // 暂停任务
        public static final int RESUME = 7;     // 恢复任务
        public static final int STOP = 8;       // 停止任务
        public static final int CANCEL = 9;     // 取消任务
        
        // 系统配置相关
        public static final int CONFIGURE = 10; // 配置/设置
        public static final int VALIDATE = 11;  // 验证/校验
        public static final int SYNC = 12;      // 同步/同步状态
        
        // 监控运维相关
        public static final int MONITOR = 13;   // 监控/检查
        public static final int HEALTH_CHECK = 14; // 健康检查
        public static final int NOTIFY = 15;    // 通知/告警
        
        // 数据管理相关
        public static final int EXPORT = 16;    // 导出数据
        public static final int IMPORT = 17;    // 导入数据
        public static final int BACKUP = 18;    // 备份数据
        public static final int RESTORE = 19;   // 恢复数据
        public static final int CLEANUP = 20;   // 清理数据
        
        // 用户认证相关
        public static final int LOGIN = 21;     // 用户登录
        public static final int LOGOUT = 22;    // 用户登出
        public static final int PERMISSION = 23; // 权限管理
        public static final int ROLE = 24;      // 角色管理
        
        // 批次管理相关
        public static final int BATCH_CREATE = 25;  // 创建批次
        public static final int BATCH_START = 26;   // 启动批次
        public static final int BATCH_PAUSE = 27;   // 暂停批次
        public static final int BATCH_RESUME = 28;  // 恢复批次
        public static final int BATCH_STOP = 29;    // 停止批次
        public static final int BATCH_CANCEL = 30;  // 取消批次
        public static final int BATCH_CLEANUP = 31; // 清理批次
        
        // 执行器管理相关
        public static final int EXECUTOR_ADD = 32;    // 添加执行器
        public static final int EXECUTOR_UPDATE = 33; // 更新执行器
        public static final int EXECUTOR_REMOVE = 34; // 移除执行器
        public static final int EXECUTOR_HEALTH = 35; // 执行器健康检查
        
        // 作业组管理相关
        public static final int GROUP_CREATE = 36;    // 创建作业组
        public static final int GROUP_UPDATE = 37;    // 更新作业组
        public static final int GROUP_DELETE = 38;    // 删除作业组
        public static final int GROUP_DISPATCH = 39;  // 作业组调度
        
        // 日志管理相关
        public static final int LOG_QUERY = 40;       // 查询日志
        public static final int LOG_DELETE = 41;      // 删除日志
        public static final int LOG_CLEANUP = 42;     // 清理日志
        public static final int LOG_EXPORT = 43;      // 导出日志
        
        // 统计分析相关
        public static final int STATS_QUERY = 44;     // 查询统计
        public static final int STATS_CALCULATE = 45; // 计算统计
        public static final int STATS_EXPORT = 46;    // 导出统计
        
        // 系统维护相关
        public static final int SYSTEM_MAINTENANCE = 47; // 系统维护
        public static final int CACHE_CLEAR = 48;        // 清理缓存
        public static final int CACHE_REFRESH = 49;      // 刷新缓存
        public static final int SYSTEM_RESTART = 50;     // 系统重启
        
        /**
         * 获取业务类型描述
         */
        public static String getDescription(int businessType) {
            switch (businessType) {
                case OTHER: return "其它/查询";
                case INSERT: return "新增/创建";
                case UPDATE: return "修改/更新";
                case DELETE: return "删除/移除";
                case EXECUTE: return "执行/运行任务";
                case SCHEDULE: return "调度/计划任务";
                case PAUSE: return "暂停任务";
                case RESUME: return "恢复任务";
                case STOP: return "停止任务";
                case CANCEL: return "取消任务";
                case CONFIGURE: return "配置/设置";
                case VALIDATE: return "验证/校验";
                case SYNC: return "同步/同步状态";
                case MONITOR: return "监控/检查";
                case HEALTH_CHECK: return "健康检查";
                case NOTIFY: return "通知/告警";
                case EXPORT: return "导出数据";
                case IMPORT: return "导入数据";
                case BACKUP: return "备份数据";
                case RESTORE: return "恢复数据";
                case CLEANUP: return "清理数据";
                case LOGIN: return "用户登录";
                case LOGOUT: return "用户登出";
                case PERMISSION: return "权限管理";
                case ROLE: return "角色管理";
                case BATCH_CREATE: return "创建批次";
                case BATCH_START: return "启动批次";
                case BATCH_PAUSE: return "暂停批次";
                case BATCH_RESUME: return "恢复批次";
                case BATCH_STOP: return "停止批次";
                case BATCH_CANCEL: return "取消批次";
                case BATCH_CLEANUP: return "清理批次";
                case EXECUTOR_ADD: return "添加执行器";
                case EXECUTOR_UPDATE: return "更新执行器";
                case EXECUTOR_REMOVE: return "移除执行器";
                case EXECUTOR_HEALTH: return "执行器健康检查";
                case GROUP_CREATE: return "创建作业组";
                case GROUP_UPDATE: return "更新作业组";
                case GROUP_DELETE: return "删除作业组";
                case GROUP_DISPATCH: return "作业组调度";
                case LOG_QUERY: return "查询日志";
                case LOG_DELETE: return "删除日志";
                case LOG_CLEANUP: return "清理日志";
                case LOG_EXPORT: return "导出日志";
                case STATS_QUERY: return "查询统计";
                case STATS_CALCULATE: return "计算统计";
                case STATS_EXPORT: return "导出统计";
                case SYSTEM_MAINTENANCE: return "系统维护";
                case CACHE_CLEAR: return "清理缓存";
                case CACHE_REFRESH: return "刷新缓存";
                case SYSTEM_RESTART: return "系统重启";
                default: return "未知类型(" + businessType + ")";
            }
        }
        
        /**
         * 获取所有业务类型
         */
        public static Map<Integer, String> getAllBusinessTypes() {
            Map<Integer, String> types = new HashMap<>();
            types.put(OTHER, getDescription(OTHER));
            types.put(INSERT, getDescription(INSERT));
            types.put(UPDATE, getDescription(UPDATE));
            types.put(DELETE, getDescription(DELETE));
            types.put(EXECUTE, getDescription(EXECUTE));
            types.put(SCHEDULE, getDescription(SCHEDULE));
            types.put(PAUSE, getDescription(PAUSE));
            types.put(RESUME, getDescription(RESUME));
            types.put(STOP, getDescription(STOP));
            types.put(CANCEL, getDescription(CANCEL));
            types.put(CONFIGURE, getDescription(CONFIGURE));
            types.put(VALIDATE, getDescription(VALIDATE));
            types.put(SYNC, getDescription(SYNC));
            types.put(MONITOR, getDescription(MONITOR));
            types.put(HEALTH_CHECK, getDescription(HEALTH_CHECK));
            types.put(NOTIFY, getDescription(NOTIFY));
            types.put(EXPORT, getDescription(EXPORT));
            types.put(IMPORT, getDescription(IMPORT));
            types.put(BACKUP, getDescription(BACKUP));
            types.put(RESTORE, getDescription(RESTORE));
            types.put(CLEANUP, getDescription(CLEANUP));
            types.put(LOGIN, getDescription(LOGIN));
            types.put(LOGOUT, getDescription(LOGOUT));
            types.put(PERMISSION, getDescription(PERMISSION));
            types.put(ROLE, getDescription(ROLE));
            types.put(BATCH_CREATE, getDescription(BATCH_CREATE));
            types.put(BATCH_START, getDescription(BATCH_START));
            types.put(BATCH_PAUSE, getDescription(BATCH_PAUSE));
            types.put(BATCH_RESUME, getDescription(BATCH_RESUME));
            types.put(BATCH_STOP, getDescription(BATCH_STOP));
            types.put(BATCH_CANCEL, getDescription(BATCH_CANCEL));
            types.put(BATCH_CLEANUP, getDescription(BATCH_CLEANUP));
            types.put(EXECUTOR_ADD, getDescription(EXECUTOR_ADD));
            types.put(EXECUTOR_UPDATE, getDescription(EXECUTOR_UPDATE));
            types.put(EXECUTOR_REMOVE, getDescription(EXECUTOR_REMOVE));
            types.put(EXECUTOR_HEALTH, getDescription(EXECUTOR_HEALTH));
            types.put(GROUP_CREATE, getDescription(GROUP_CREATE));
            types.put(GROUP_UPDATE, getDescription(GROUP_UPDATE));
            types.put(GROUP_DELETE, getDescription(GROUP_DELETE));
            types.put(GROUP_DISPATCH, getDescription(GROUP_DISPATCH));
            types.put(LOG_QUERY, getDescription(LOG_QUERY));
            types.put(LOG_DELETE, getDescription(LOG_DELETE));
            types.put(LOG_CLEANUP, getDescription(LOG_CLEANUP));
            types.put(LOG_EXPORT, getDescription(LOG_EXPORT));
            types.put(STATS_QUERY, getDescription(STATS_QUERY));
            types.put(STATS_CALCULATE, getDescription(STATS_CALCULATE));
            types.put(STATS_EXPORT, getDescription(STATS_EXPORT));
            types.put(SYSTEM_MAINTENANCE, getDescription(SYSTEM_MAINTENANCE));
            types.put(CACHE_CLEAR, getDescription(CACHE_CLEAR));
            types.put(CACHE_REFRESH, getDescription(CACHE_REFRESH));
            types.put(SYSTEM_RESTART, getDescription(SYSTEM_RESTART));
            return types;
        }
    }

    /**
     * 操作类别常量
     */
    public static final class OperatorType {
        public static final int OTHER = 0;
        public static final int ADMIN = 1;
        public static final int MOBILE = 2;
    }

    /**
     * 操作状态常量
     */
    public static final class Status {
        public static final int SUCCESS = 0;
        public static final int FAIL = 1;
    }
}