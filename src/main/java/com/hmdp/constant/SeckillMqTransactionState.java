package com.hmdp.constant;

/**
 * Redis 中保存的 RocketMQ 本地事务状态。
 */
public final class SeckillMqTransactionState {

    public static final String COMMIT = "COMMIT";
    public static final String ROLLBACK_OUT_OF_STOCK = "ROLLBACK:OUT_OF_STOCK";
    public static final String ROLLBACK_DUPLICATE = "ROLLBACK:DUPLICATE";

    private SeckillMqTransactionState() {
    }
}
