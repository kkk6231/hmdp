package com.hmdp.mq;

import com.hmdp.constant.MQConstants;
import com.hmdp.constant.SeckillRedisKeys;
import com.hmdp.dto.SeckillVoucherTransactionContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.messaging.Message;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.hmdp.constant.SeckillLuaResultCode.DUPLICATE_ORDER;
import static com.hmdp.constant.SeckillLuaResultCode.OUT_OF_STOCK;
import static com.hmdp.constant.SeckillLuaResultCode.SUCCESS;
import static com.hmdp.constant.SeckillMqTransactionState.COMMIT;
import static com.hmdp.constant.SeckillMqTransactionState.ROLLBACK_DUPLICATE;
import static com.hmdp.constant.SeckillMqTransactionState.ROLLBACK_OUT_OF_STOCK;
import static com.hmdp.constant.SeckillRedisKeys.MQ_TRANSACTION_TTL_SECONDS;
import static com.hmdp.constant.SeckillRedisKeys.ORDER_PENDING_KEY;

@Slf4j
@RocketMQTransactionListener
public class SeckillVoucherTransactionListener implements RocketMQLocalTransactionListener {

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final DefaultRedisScript<Long> ORDER_PROCESSING_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("lua/seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        ORDER_PROCESSING_SCRIPT = new DefaultRedisScript<>();
        ORDER_PROCESSING_SCRIPT.setLocation(
                new ClassPathResource("lua/seckill_order_processing.lua"));
        ORDER_PROCESSING_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 仅用于验证 Broker 事务回查：Lua 成功后强制返回 UNKNOWN。
     * 正常运行必须保持为 false。
     */
    @Value("${hmdp.mq.fault-injection.force-transaction-unknown-after-lua:false}")
    private boolean forceTransactionUnknownAfterLua;

    /**
     * 执行本地事务
     * @param message
     * @param arg
     * @return
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object arg) {
        if (!(arg instanceof SeckillVoucherTransactionContext)) {
            log.error("秒杀事务消息缺少本地事务上下文，messageHeaders={}", message.getHeaders());
            return RocketMQLocalTransactionState.UNKNOWN;
        }

        SeckillVoucherTransactionContext context = (SeckillVoucherTransactionContext) arg;
        try {
            Long result = executeSeckillLua(
                    context.getOrderId(), context.getUserId(), context.getVoucherId());
            if (result == null) {
                log.error("秒杀 Lua 未返回结果，orderId={}", context.getOrderId());
                return RocketMQLocalTransactionState.UNKNOWN;
            }

            int resultCode = result.intValue();
            context.setLuaResult(resultCode);
            if (resultCode == SUCCESS) {
                if (forceTransactionUnknownAfterLua) {
                    log.warn("[故障注入] Lua 已成功，强制返回 UNKNOWN，等待 Broker 回查，orderId={}",
                            context.getOrderId());
                    return RocketMQLocalTransactionState.UNKNOWN;
                }
                log.info("秒杀本地事务执行成功，提交事务消息，orderId={}", context.getOrderId());
                return RocketMQLocalTransactionState.COMMIT;
            }
            if (resultCode == OUT_OF_STOCK || resultCode == DUPLICATE_ORDER) {
                log.info("秒杀业务校验未通过，回滚事务消息，orderId={}，result={}",
                        context.getOrderId(), resultCode);
                return RocketMQLocalTransactionState.ROLLBACK;
            }

            log.error("秒杀 Lua 返回未知结果，orderId={}，result={}",
                    context.getOrderId(), resultCode);
            return RocketMQLocalTransactionState.UNKNOWN;
        } catch (Exception e) {
            // Redis 超时不代表 Lua 一定没有执行，不能直接回滚事务消息。
            log.error("秒杀本地事务执行异常，等待 Broker 回查，orderId={}",
                    context.getOrderId(), e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    /**
     * 事务回查
     * @param message
     * @return
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        try {
            Long orderId = getLongHeader(message, MQConstants.SECKILL_ORDER_ID_HEADER);
            Long userId = getLongHeader(message, MQConstants.SECKILL_USER_ID_HEADER);
            Long voucherId = getLongHeader(message, MQConstants.SECKILL_VOUCHER_ID_HEADER);

            String transactionKey = SeckillRedisKeys.transactionKey(orderId);
            String transactionState = stringRedisTemplate.opsForValue().get(transactionKey);
            if (COMMIT.equals(transactionState)) {
                ensureOrderProcessing(orderId, userId, voucherId);
                log.info("事务回查确认提交，orderId={}", orderId);
                return RocketMQLocalTransactionState.COMMIT;
            }
            if (ROLLBACK_OUT_OF_STOCK.equals(transactionState)
                    || ROLLBACK_DUPLICATE.equals(transactionState)) {
                log.info("事务回查确认回滚，orderId={}，transactionState={}",
                        orderId, transactionState);
                return RocketMQLocalTransactionState.ROLLBACK;
            }

            // 事务状态丢失时，再用 userId -> orderId 的资格记录进行一次兜底判断。
            String userOrderKey = SeckillRedisKeys.userOrderKey(voucherId);
            Object existingOrderId = stringRedisTemplate.opsForHash().get(
                    userOrderKey, String.valueOf(userId));
            if (existingOrderId != null && String.valueOf(orderId).equals(existingOrderId.toString())) {
                stringRedisTemplate.opsForValue().set(
                        transactionKey, COMMIT, MQ_TRANSACTION_TTL_SECONDS, TimeUnit.SECONDS);
                ensureOrderProcessing(orderId, userId, voucherId);
                log.warn("事务状态缺失，通过用户订单映射恢复 COMMIT，orderId={}", orderId);
                return RocketMQLocalTransactionState.COMMIT;
            }
            if (existingOrderId != null) {
                stringRedisTemplate.opsForValue().set(
                        transactionKey, ROLLBACK_DUPLICATE,
                        MQ_TRANSACTION_TTL_SECONDS, TimeUnit.SECONDS);
                log.info("事务状态缺失且用户已绑定其他订单，确认回滚，orderId={}，existingOrderId={}",
                        orderId, existingOrderId);
                return RocketMQLocalTransactionState.ROLLBACK;
            }

            log.warn("事务回查暂时无法确定结果，orderId={}", orderId);
            return RocketMQLocalTransactionState.UNKNOWN;
        } catch (Exception e) {
            log.error("事务回查异常，保持 UNKNOWN，messageHeaders={}", message.getHeaders(), e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    private Long executeSeckillLua(Long orderId, Long userId, Long voucherId) {
        String stockKey = SeckillRedisKeys.stockKey(voucherId);
        String userOrderKey = SeckillRedisKeys.userOrderKey(voucherId);
        String transactionKey = SeckillRedisKeys.transactionKey(orderId);
        String orderResultKey = SeckillRedisKeys.orderResultKey(orderId);
        return stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Arrays.asList(
                        stockKey, userOrderKey, transactionKey,
                        orderResultKey, ORDER_PENDING_KEY),
                String.valueOf(userId), String.valueOf(orderId), String.valueOf(voucherId),
                String.valueOf(MQ_TRANSACTION_TTL_SECONDS),
                String.valueOf(System.currentTimeMillis())
        );
    }

    /**
     * 回查确认 Redis 资格已成功时，补齐可能丢失的 PROCESSING 状态。
     * 已经结束的 SUCCESS/FAILED 状态不能被降级。
     */
    private void ensureOrderProcessing(Long orderId, Long userId, Long voucherId) {
        String resultKey = SeckillRedisKeys.orderResultKey(orderId);
        stringRedisTemplate.execute(
                ORDER_PROCESSING_SCRIPT,
                Arrays.asList(resultKey, ORDER_PENDING_KEY),
                String.valueOf(orderId), String.valueOf(userId), String.valueOf(voucherId),
                String.valueOf(System.currentTimeMillis()));
    }

    private Long getLongHeader(Message message, String headerName) {
        Object value = message.getHeaders().get(headerName);
        if (value == null) {
            throw new IllegalArgumentException("事务消息缺少 Header：" + headerName);
        }
        return Long.valueOf(value.toString());
    }
}
