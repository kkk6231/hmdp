package com.hmdp.mq;

import com.hmdp.constant.MQConstants;
import com.hmdp.constant.SeckillRedisKeys;
import com.hmdp.dto.SeckillVoucherTransactionContext;
import com.hmdp.service.ISeckillVoucherService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.Message;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.hmdp.constant.SeckillLuaResultCode.DUPLICATE_ORDER;
import static com.hmdp.constant.SeckillLuaResultCode.OUT_OF_STOCK;
import static com.hmdp.constant.SeckillLuaResultCode.SUCCESS;
import static com.hmdp.constant.SeckillLuaResultCode.ACTIVITY_ENDED;
import static com.hmdp.constant.SeckillLuaResultCode.ACTIVITY_NOT_READY;
import static com.hmdp.constant.SeckillLuaResultCode.ACTIVITY_NOT_STARTED;
import static com.hmdp.constant.SeckillMqTransactionState.COMMIT;
import static com.hmdp.constant.SeckillMqTransactionState.ROLLBACK_DUPLICATE;
import static com.hmdp.constant.SeckillMqTransactionState.ROLLBACK_ENDED;
import static com.hmdp.constant.SeckillMqTransactionState.ROLLBACK_NOT_READY;
import static com.hmdp.constant.SeckillMqTransactionState.ROLLBACK_NOT_STARTED;
import static com.hmdp.constant.SeckillMqTransactionState.ROLLBACK_OUT_OF_STOCK;
import static com.hmdp.constant.SeckillRedisKeys.MQ_TRANSACTION_TTL_SECONDS;

/**
 * RocketMQ 事务消息适配层：将秒杀券服务的资格预留结果映射成消息事务状态，
 * 并在 Broker 回查时根据 Redis 中持久化的事实恢复提交或回滚决策。
 */
@Slf4j
@RocketMQTransactionListener
public class SeckillVoucherTransactionListener implements RocketMQLocalTransactionListener {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    /**
     * Half Message 写入 Broker 后执行 Redis 资格预留。
     * Lua 明确成功才提交；明确的业务拒绝才回滚；无法判断时交给 Broker 回查。
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object arg) {
        if (!(arg instanceof SeckillVoucherTransactionContext)) {
            log.error("秒杀事务消息缺少本地事务上下文，messageHeaders={}", message.getHeaders());
            return RocketMQLocalTransactionState.UNKNOWN;
        }

        SeckillVoucherTransactionContext context = (SeckillVoucherTransactionContext) arg;
        try {
            Long result = seckillVoucherService.reserveQualification(
                    context.getOrderId(), context.getUserId(), context.getVoucherId());
            if (result == null) {
                log.error("秒杀 Lua 未返回结果，orderId={}", context.getOrderId());
                return RocketMQLocalTransactionState.UNKNOWN;
            }

            // 同步发送线程通过 context 读取结果并决定给用户的返回内容。
            int resultCode = result.intValue();
            context.setLuaResult(resultCode);
            if (resultCode == SUCCESS) {
                log.info("秒杀本地事务执行成功，提交事务消息，orderId={}", context.getOrderId());
                return RocketMQLocalTransactionState.COMMIT;
            }
            if (resultCode == OUT_OF_STOCK
                    || resultCode == DUPLICATE_ORDER
                    || resultCode == ACTIVITY_NOT_STARTED
                    || resultCode == ACTIVITY_ENDED
                    || resultCode == ACTIVITY_NOT_READY) {
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
     * Broker 收不到明确事务决议时回查 Redis。
     * 回查不会再次预扣库存，只读取事务状态或用户资格映射。
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        try {
            Long orderId = getLongHeader(message, MQConstants.SECKILL_ORDER_ID_HEADER);
            Long userId = getLongHeader(message, MQConstants.SECKILL_USER_ID_HEADER);
            Long voucherId = getLongHeader(message, MQConstants.SECKILL_VOUCHER_ID_HEADER);

            // 优先使用 Lua 写下的事务结论，避免把业务回滚误判为成功。
            String transactionKey = SeckillRedisKeys.transactionKey(orderId);
            String transactionState = stringRedisTemplate.opsForValue().get(transactionKey);
            if (COMMIT.equals(transactionState)) {
                seckillVoucherService.ensureOrderProcessing(orderId, userId, voucherId);
                log.info("事务回查确认提交，orderId={}", orderId);
                return RocketMQLocalTransactionState.COMMIT;
            }
            if (ROLLBACK_OUT_OF_STOCK.equals(transactionState)
                    || ROLLBACK_DUPLICATE.equals(transactionState)
                    || ROLLBACK_NOT_STARTED.equals(transactionState)
                    || ROLLBACK_ENDED.equals(transactionState)
                    || ROLLBACK_NOT_READY.equals(transactionState)) {
                log.info("事务回查确认回滚，orderId={}，transactionState={}",
                        orderId, transactionState);
                return RocketMQLocalTransactionState.ROLLBACK;
            }

            // 事务状态丢失时，再用 userId -> orderId 的资格记录进行一次兜底判断。
            String existingOrderId = seckillVoucherService.findExistingOrderIdValue(userId, voucherId);
            if (existingOrderId != null && String.valueOf(orderId).equals(existingOrderId)) {
                stringRedisTemplate.opsForValue().set(
                        transactionKey, COMMIT, MQ_TRANSACTION_TTL_SECONDS, TimeUnit.SECONDS);
                seckillVoucherService.ensureOrderProcessing(orderId, userId, voucherId);
                log.warn("事务状态缺失，通过用户订单映射恢复 COMMIT，orderId={}", orderId);
                return RocketMQLocalTransactionState.COMMIT;
            }
            // 用户资格指向另一个订单时，当前 Half Message 不应被投递。
            if (existingOrderId != null) {
                stringRedisTemplate.opsForValue().set(
                        transactionKey, ROLLBACK_DUPLICATE,
                        MQ_TRANSACTION_TTL_SECONDS, TimeUnit.SECONDS);
                log.info("事务状态缺失且用户已绑定其他订单，确认回滚，orderId={}，existingOrderId={}",
                        orderId, existingOrderId);
                return RocketMQLocalTransactionState.ROLLBACK;
            }

            // 两种证据都不存在时不能猜测结果；继续 UNKNOWN 等待后续回查。
            log.warn("事务回查暂时无法确定结果，orderId={}", orderId);
            return RocketMQLocalTransactionState.UNKNOWN;
        } catch (Exception e) {
            log.error("事务回查异常，保持 UNKNOWN，messageHeaders={}", message.getHeaders(), e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    /** 回查没有发送线程的 context，因此必须从持久化的消息 Header 还原业务 ID。 */
    private Long getLongHeader(Message message, String headerName) {
        Object value = message.getHeaders().get(headerName);
        if (value == null) {
            throw new IllegalArgumentException("事务消息缺少 Header：" + headerName);
        }
        return Long.valueOf(value.toString());
    }
}
