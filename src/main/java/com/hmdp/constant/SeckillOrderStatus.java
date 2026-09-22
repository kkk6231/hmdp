package com.hmdp.constant;

/**
 * 秒杀订单异步处理状态。
 */
public enum SeckillOrderStatus {
    /** Redis 资格校验已通过，等待消费者创建数据库订单。 */
    PROCESSING,

    /** 数据库事务已经提交，订单创建成功。 */
    SUCCESS,

    /** 已明确确认订单最终失败，预留给后续 DLQ 人工处理流程。 */
    FAILED
}
