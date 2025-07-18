
CREATE TABLE job_def (
    job_id         BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_code       VARCHAR(100) NOT NULL UNIQUE COMMENT '作业编码',
    job_name       VARCHAR(200) NOT NULL COMMENT '作业名称',
    proc_name      VARCHAR(200) COMMENT '存储过程名',
    job_type       VARCHAR(50) COMMENT '作业类型（PROC/SCRIPT/SQL）',
    schedule_time TIME,                            -- 调度时间
    status VARCHAR(20) DEFAULT 'active',           -- 当前状态（active, inactive）
    timeout_sec    INT DEFAULT 1800 COMMENT '超时时间（秒）',
    retry_count    INT DEFAULT 0 COMMENT '失败重试次数',
    notify_email   VARCHAR(500) COMMENT '失败通知邮箱',
    is_depend      TINYINT(1) DEFAULT 1, '1 有依赖 0 没有依赖'
    is_active      TINYINT(1) DEFAULT 1,
    last_run_time DATETIME,                        -- 上次执行时间
    next_run_time DATETIME                         -- 下次执行时间
    create_time    DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) COMMENT='作业定义表';

CREATE TABLE job_dependency (
    id             BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_code       VARCHAR(100) NOT NULL COMMENT '当前作业',
    depends_on     VARCHAR(100) NOT NULL COMMENT '依赖的作业',
    dependency_order INT,                          -- 依赖顺序，决定执行的优先级
    UNIQUE KEY uq_job_dep (job_code, depends_on)
) COMMENT='作业依赖关系表';


CREATE TABLE job_queue (
    queue_id       BIGINT PRIMARY KEY AUTO_INCREMENT,
    batch_no       VARCHAR(50) NOT NULL COMMENT '批次号',
    job_code       VARCHAR(100) NOT NULL COMMENT '作业编码',
    status         VARCHAR(20) DEFAULT 'PENDING' COMMENT '状态 PENDING/RUNNING/SUCCESS/FAILED/SKIPPED',
    try_count      INT DEFAULT 0 COMMENT '已重试次数',
    error_message  TEXT,
    create_time    DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_batch_job (batch_no, job_code)
) COMMENT='作业队列表';

CREATE TABLE job_execution_logs (
    log_id INT PRIMARY KEY AUTO_INCREMENT,        -- 日志ID
    queue_id INT,                                  -- 任务ID
    start_time DATETIME,                          -- 开始执行时间
    end_time DATETIME,                            -- 结束执行时间
    status VARCHAR(20),                           -- 执行状态（success, failed）s
    error_message TEXT,                           -- 错误信息（如失败时）
    duration INT,                                 -- 秒 执行时间
    notify_status VARCHAR(20),                    -- NOTIFIED/UNNOTIFIED --通知状态
    retry_count INT,                              -- 重试次数
    FOREIGN KEY (task_id) REFERENCES task_queue(task_id)
);

CREATE TABLE email_notifications (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    recipient_email VARCHAR(255) NOT NULL, --接收者邮箱
    subject VARCHAR(255) NOT NULL,  --邮件主题
    body TEXT NOT NULL, --邮件内容（纯文本或HTML）
    status ENUM('pending', 'sent', 'failed') NOT NULL DEFAULT 'pending', --发送状态：pending sent failed
    send_time DATETIME NULL, --发送时间
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, --创建时间
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,--更新时间
    fail_reason TEXT NULL,--失败原因
    type VARCHAR(50) NULL,--通知类型 
    retry_count INT NOT NULL DEFAULT 0, --重试次数
    is_system BOOLEAN NOT NULL DEFAULT TRUE, --是否系统自动通知
    INDEX idx_recipient_email (recipient_email),
    INDEX idx_status (status),
    INDEX idx_send_time (send_time)
);


处理流程
1. xxl-job根据corn定义调度作业Handler  queueDispatcherHandler
2. queueDispatcherHandler 扫描job_queue，找出所有依赖满足、状态为PENDING的作业
3. 程序可并发执行无依赖作业，串行执行有依赖作业.在这个处理过程中可以有两种方式，
   1.是根据队列中作业情况。符合条件的作业生成一条一次执行的任务交给xxl-job进行处理，
     queueDispatcherHandler只负责扫描job_queue找出符合条件的任务进行执行。根据作业任务的状态更新日子，发送通知。
   2. 多任务任务的执行都在queueDispatcherHandler 进行处理，xxl-job只做处罚
    推荐按照1进行处理
4. 每次作业状态变更写入job_execution_logs
5. 作业执行完成发送邮件通知或出发下一级信号


1. XXL-JOB 配置部分
1.1 任务配置（调度中心Web界面/数据库）
作业Handler名称（queueDispatcherHandler）
调度频率（Cron表达式，支持月、季、年、临时等）
任务参数（如批次号、作业类型等，JSON格式推荐）
路由策略（如第一个、轮询、分片等，通常选“第一个”或“分片广播”）
阻塞处理策略（如单机串行、丢弃后续、覆盖之前等）
失败重试次数（如3次）
超时时间（如1800秒）
负责人/报警邮箱（用于失败通知）
依赖关系（如有，需在业务表/配置中维护）
示例：
配置项	示例值
JobHandler	queueDispatcherHandler
Cron	0 0 2 1 * ? （每月1日2点）
参数	{"batchNo":"202404","type":"monthly"}
失败重试	3
超时时间	3600
负责人	zxr@aia.com

1.2 配置部分的作用
调度中心负责：定时触发任务、传递参数、记录日志、失败重试、邮件通知
配置只负责“调度入口”，不负责依赖关系、队列出队、作业状态流转等复杂业务

2. 需要程序处理的逻辑部分
2.1 依赖关系与队列管理
依赖关系解析：程序根据job_dependency表或配置，判断哪些作业可执行
队列出队：程序扫描job_queue，找出所有依赖满足、状态为PENDING的作业
并发/串行控制：程序可并发执行无依赖作业，串行执行有依赖作业
作业状态流转：PENDING→RUNNING→SUCCESS/FAILED，失败可重试
批次管理：支持多批次并发调度
日志与告警：程序记录详细执行日志，失败/超时自动邮件通知
依赖链可视化：程序可生成依赖DAG数据供前端展示



2.2 程序逻辑流程
调度入口（由XXL-JOB配置定时触发）
解析参数（如批次号、账期）
生成本批次的job_queue（如每月1日生成本月所有作业队列）
队列调度循环
查询所有status='PENDING'且依赖全部SUCCESS的作业
并发/串行调度这些作业
每个作业执行时，调用对应存储过程/脚本
执行成功则标记SUCCESS，失败则重试或标记FAILED
失败/超时自动邮件通知
所有作业完成后，记录批次完成状态
依赖关系处理
依赖关系由job_dependency表维护，程序每次出队前检查依赖
支持多级依赖、并行、串行、跳过等策略
日志与监控
每次作业状态变更写入job_execution_logs
支持查询、追溯、可视化

3. 配置与程序分工总结
功能	配置部分（XXL-JOB）	程序处理部分（业务代码/执行器）
调度频率	Cron表达式	-
任务参数	Web界面/JSON	解析参数，生成队列
失败重试	配置重试次数	具体重试逻辑、状态流转
邮件通知	配置负责人/邮箱	失败/超时时自动发邮件
依赖关系	-（推荐用业务表维护）	依赖检查、DAG调度、并发/串行控制
队列管理	-	队列表生成、出队、状态流转
日志与监控	XXL-JOB日志	详细作业日志、队列日志、可视化数据
依赖可视化	-	生成DAG数据供前端展示
多批次/多账期	-	队列表按批次/账期隔离





while true:
    ready_jobs = select * from job_queue where status='PENDING' and all dependencies SUCCESS
    for job in ready_jobs:
        mark job as RUNNING
        result = 调用存储过程/脚本
        if result.success:
            mark job as SUCCESS
        else:
            if job.try_count < job.retry_count:
                mark job as PENDING, try_count++
            else:
                mark job as FAILED
                send_mail(job.notify_email, ...)
    if no more jobs to run:
        break