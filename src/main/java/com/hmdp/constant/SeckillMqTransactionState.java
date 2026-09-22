package com.hmdp.constant;

/**
 * Redis 中保存的 RocketMQ 本地事务状态。
 */
public final class SeckillMqTransactionState {

    public static final String COMMIT = "COMMIT";
    public static final String ROLLBACK_OUT_OF_STOCK = "ROLLBACK:OUT_OF_STOCK";
    public static final String ROLLBACK_DUPLICATE = "ROLLBACK:DUPLICATE";
    public static final String ROLLBACK_NOT_STARTED = "ROLLBACK:NOT_STARTED";
    public static final String ROLLBACK_ENDED = "ROLLBACK:ENDED";
    public static final String ROLLBACK_NOT_READY = "ROLLBACK:NOT_READY";

    private SeckillMqTransactionState() {
    }
}
