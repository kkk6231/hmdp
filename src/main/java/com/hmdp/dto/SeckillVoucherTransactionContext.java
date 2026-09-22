package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 事务消息发送线程与本地事务回调之间共享的上下文。
 * Broker 发起事务回查时不会携带该对象，回查依据必须持久化在 Redis 和消息 Header 中。
 */
@Data
@AllArgsConstructor
public class SeckillVoucherTransactionContext {

    private final Long orderId;
    private final Long userId;
    private final Long voucherId;

    /**
     * Lua 业务返回码；null 表示本地事务结果尚不能确定。
     */
    private volatile Integer luaResult;

    public SeckillVoucherTransactionContext(Long orderId, Long userId, Long voucherId) {
        this(orderId, userId, voucherId, null);
    }
}
