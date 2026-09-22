package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.constant.MQConstants;
import com.hmdp.constant.SeckillOrderStatus;
import com.hmdp.constant.SeckillRedisKeys;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderStatusDTO;
import com.hmdp.dto.SeckillVoucherMqDTO;
import com.hmdp.dto.SeckillVoucherTransactionContext;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.constant.SeckillLuaResultCode.DUPLICATE_ORDER;
import static com.hmdp.constant.SeckillLuaResultCode.OUT_OF_STOCK;
import static com.hmdp.constant.SeckillLuaResultCode.SUCCESS;
import static com.hmdp.constant.SeckillRedisKeys.ORDER_FAILED_TTL_SECONDS;
import static com.hmdp.constant.SeckillRedisKeys.ORDER_PENDING_KEY;
import static com.hmdp.constant.SeckillRedisKeys.ORDER_SUCCESS_TTL_SECONDS;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final DefaultRedisScript<Long> ORDER_SUCCESS_SCRIPT;
    private static final DefaultRedisScript<Long> ORDER_COMPENSATE_SCRIPT;

    static {
        ORDER_SUCCESS_SCRIPT = new DefaultRedisScript<>();
        ORDER_SUCCESS_SCRIPT.setLocation(
                new ClassPathResource("lua/seckill_order_success.lua"));
        ORDER_SUCCESS_SCRIPT.setResultType(Long.class);

        ORDER_COMPENSATE_SCRIPT = new DefaultRedisScript<>();
        ORDER_COMPENSATE_SCRIPT.setLocation(
                new ClassPathResource("lua/seckill_order_compensate.lua"));
        ORDER_COMPENSATE_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RocketMQTemplate rocketMQTemplate;
    @Resource
    private TransactionTemplate transactionTemplate;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    @Value("${hmdp.mq.order-lock-wait-millis:200}")
    private long orderLockWaitMillis;

    /**
     * 故障注入仅用于验证消费重试和 DLQ，正常运行必须保持为 false。
     */
    @Value("${hmdp.mq.fault-injection.force-consumer-failure-before-db:false}")
    private boolean forceConsumerFailureBeforeDb;

    /**
     * 故障注入仅用于验证数据库已提交、Redis SUCCESS 尚未写入时的幂等恢复。
     */
    @Value("${hmdp.mq.fault-injection.force-consumer-failure-after-db-commit:false}")
    private boolean forceConsumerFailureAfterDbCommit;


    /**
     * 优惠券秒杀
     * @param voucherId
     * @return
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order"); // 生成订单号

        // 消费者最终会收到的消息
        SeckillVoucherMqDTO seckillVoucherMqDTO = SeckillVoucherMqDTO.builder()
                .orderId(orderId)
                .userId(userId)
                .voucherId(voucherId)
                .build();

        Message<SeckillVoucherMqDTO> message = MessageBuilder
                .withPayload(seckillVoucherMqDTO)
                .setHeader(RocketMQHeaders.KEYS, String.valueOf(orderId))
                .setHeader(MQConstants.SECKILL_ORDER_ID_HEADER, String.valueOf(orderId))
                .setHeader(MQConstants.SECKILL_USER_ID_HEADER, String.valueOf(userId))
                .setHeader(MQConstants.SECKILL_VOUCHER_ID_HEADER, String.valueOf(voucherId))
                .build();

        // 本地事务上下文，用于存放 lua 执行结果
        SeckillVoucherTransactionContext context =
                new SeckillVoucherTransactionContext(orderId, userId, voucherId);

        try {
            // 发送事务消息
            /**
             * 1. 将 Half Message发送给 Broker
             * 2. Broker保存 Half Message
             * 3. 调用 executeLocalTransaction(message, context)
             * 4. 将 COMMIT/ROLLBACK/UNKNOWN 通知 Broker
             * 5. 返回 TransactionSendResult
             */
            TransactionSendResult sendResult = rocketMQTemplate.sendMessageInTransaction(
                    MQConstants.SECKILL_VOUCHER_TOPIC,
                    message,
                    context
            );

            if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
                log.error("秒杀事务消息发送状态异常，orderId={}，sendStatus={}，localState={}",
                        orderId,
                        sendResult == null ? null : sendResult.getSendStatus(),
                        sendResult == null ? null : sendResult.getLocalTransactionState());
                return Result.fail("系统繁忙，请稍后重试");
            }
        } catch (MessagingException e) {
            log.error("秒杀事务消息发送失败，orderId={}，userId={}，voucherId={}",
                    orderId, userId, voucherId, e);
            if (context.getLuaResult() != null) {
                return resolveSeckillResult(orderId, context.getLuaResult());
            }
            return Result.fail("系统繁忙，请稍后重试");
        }

        return resolveSeckillResult(orderId, context.getLuaResult());
    }

    /**
     * 秒杀结果判断
     * @param orderId
     * @param luaResult
     * @return
     */
    private Result resolveSeckillResult(long orderId, Integer luaResult) {
        if (luaResult == null) {
            // 本地事务返回 UNKNOWN，最终结果由 Broker 回查决定。
            log.warn("秒杀本地事务状态未知，返回订单号供后续查询，orderId={}", orderId);
            return Result.ok(orderId);
        }
        if (luaResult == OUT_OF_STOCK) {
            return Result.fail("库存不足");
        }
        if (luaResult == DUPLICATE_ORDER) {
            return Result.fail("不能重复下单");
        }
        if (luaResult != SUCCESS) {
            log.error("秒杀 Lua 返回未知结果，orderId={}，result={}", orderId, luaResult);
            return Result.fail("秒杀服务异常，请稍后重试");
        }
        return Result.ok(orderId);
    }

    @Override
    public Result querySeckillOrderStatus(Long orderId) {
        if (orderId == null) {
            return Result.fail("订单号不能为空");
        }

        Long currentUserId = UserHolder.getUser().getId();
        String resultKey = SeckillRedisKeys.orderResultKey(orderId);
        boolean redisAvailable = true;
        try {
            Map<Object, Object> result = stringRedisTemplate.opsForHash().entries(resultKey);
            if (!result.isEmpty()) {
                Long resultUserId = parseLong(result.get("userId"));
                if (resultUserId == null || !currentUserId.equals(resultUserId)) {
                    return Result.fail("无权查询该订单");
                }

                SeckillOrderStatus status = parseOrderStatus(result.get("status"));
                Long voucherId = parseLong(result.get("voucherId"));
                if (status != null) {
                    return Result.ok(buildOrderStatus(orderId, voucherId, status));
                }
            }
        } catch (Exception e) {
            redisAvailable = false;
            log.warn("查询 Redis 秒杀订单状态失败，回退查询数据库，orderId={}", orderId, e);
        }

        VoucherOrder voucherOrder = getById(orderId);
        if (voucherOrder != null) {
            if (!currentUserId.equals(voucherOrder.getUserId())) {
                return Result.fail("无权查询该订单");
            }

            try {
                markOrderSuccess(
                        voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId());
            } catch (Exception e) {
                log.warn("数据库订单存在但 Redis SUCCESS 状态回填失败，orderId={}", orderId, e);
            }
            return Result.ok(buildOrderStatus(
                    voucherOrder.getId(), voucherOrder.getVoucherId(), SeckillOrderStatus.SUCCESS));
        }

        if (!redisAvailable) {
            return Result.fail("订单状态暂时不可用，请稍后重试");
        }
        return Result.fail("订单不存在或处理结果已过期");
    }

    /**
     * 扣减库存 + 创建订单 （消费者调用）
     * @param message
     */
    @Override
    public void createVoucherOrder(SeckillVoucherMqDTO message) {
        validateMessage(message);
        if (forceConsumerFailureBeforeDb) {
            log.warn("[故障注入] 消费者在数据库事务前强制失败，orderId={}",
                    message.getOrderId());
            throw new IllegalStateException(
                    "[故障注入] 消费者在数据库事务前强制失败，orderId=" + message.getOrderId());
        }

        executeWithOrderLock(
                message.getOrderId(),
                () -> doCreateVoucherOrder(message));
    }

    private void doCreateVoucherOrder(SeckillVoucherMqDTO message) {
        SeckillOrderStatus redisStatus = getStoredOrderStatus(message.getOrderId());

        // 用于处理已经消费成功，但是重复发消息的情况，例如 ACK 失败
        VoucherOrder existingOrder = getById(message.getOrderId()); // 根据 orderId 查订单
        if (existingOrder != null) {
            if (isSameOrder(existingOrder, message)) { // 如果 userId 和 voucherId 相同，说明是重复消息，直接忽略
                if (redisStatus == SeckillOrderStatus.FAILED) {
                    throw new IllegalStateException(
                            "数据库订单存在但 Redis 状态为 FAILED，orderId=" + message.getOrderId());
                }
                markOrderSuccess(message.getOrderId(), message.getUserId(), message.getVoucherId());
                log.info("订单消息重复投递，忽略处理，orderId={}", message.getOrderId());
                return;
            }
            throw new IllegalStateException("订单号已被其他订单占用，orderId=" + message.getOrderId());
        }

        if (redisStatus == SeckillOrderStatus.FAILED) {
            log.warn("订单已经完成库存补偿，忽略迟到或人工重放消息，orderId={}",
                    message.getOrderId());
            return;
        }
        if (redisStatus == SeckillOrderStatus.SUCCESS) {
            throw new IllegalStateException(
                    "Redis 状态为 SUCCESS 但数据库订单不存在，orderId=" + message.getOrderId());
        }

        try {
            Boolean created = transactionTemplate.execute(status -> {
                boolean stockUpdated = seckillVoucherService.update()
                        .setSql("stock = stock - 1")
                        .eq("voucher_id", message.getVoucherId())
                        .gt("stock", 0)
                        .update();
                if (!stockUpdated) {
                    throw new IllegalStateException(
                            "数据库秒杀库存不足，voucherId=" + message.getVoucherId());
                }

                VoucherOrder voucherOrder = new VoucherOrder()
                        .setId(message.getOrderId())
                        .setUserId(message.getUserId())
                        .setVoucherId(message.getVoucherId());
                if (!save(voucherOrder)) {
                    throw new IllegalStateException(
                            "秒杀订单保存失败，orderId=" + message.getOrderId());
                }
                return true;
            });

            if (!Boolean.TRUE.equals(created)) {
                throw new IllegalStateException(
                        "秒杀订单事务执行结果异常，orderId=" + message.getOrderId());
            }
            if (forceConsumerFailureAfterDbCommit) {
                log.warn("[故障注入] 数据库事务已提交，在写入 Redis SUCCESS 前强制失败，orderId={}",
                        message.getOrderId());
                throw new IllegalStateException(
                        "[故障注入] 数据库事务提交后强制失败，orderId=" + message.getOrderId());
            }

            log.info("秒杀订单创建成功，orderId={}，userId={}，voucherId={}",
                    message.getOrderId(), message.getUserId(), message.getVoucherId());
            markOrderSuccess(message.getOrderId(), message.getUserId(), message.getVoucherId());
            // 订单 id 冲突 或者 联合唯一索引冲突
        } catch (DuplicateKeyException e) {
            // 用于处理两个消费者并发处理相同消息的情况
            VoucherOrder duplicateOrder = getById(message.getOrderId());
            if (duplicateOrder != null && isSameOrder(duplicateOrder, message)) { // 说明是重复消费
                markOrderSuccess(message.getOrderId(), message.getUserId(), message.getVoucherId());
                log.info("并发重复消费命中订单唯一键，按消费成功处理，orderId={}",
                        message.getOrderId());
                return;
            }

            log.error("秒杀订单唯一键冲突，orderId={}，userId={}，voucherId={}",
                    message.getOrderId(), message.getUserId(), message.getVoucherId(), e);
            throw e;
        }
    }

    @Override
    public void handleDeadLetter(SeckillVoucherMqDTO message) {
        validateMessage(message);

        executeWithOrderLock(
                message.getOrderId(),
                () -> doHandleDeadLetter(message));
    }

    private void doHandleDeadLetter(SeckillVoucherMqDTO message) {
        VoucherOrder existingOrder = getById(message.getOrderId());
        if (existingOrder != null) {
            if (!isSameOrder(existingOrder, message)) {
                throw new IllegalStateException(
                        "死信订单号已被其他订单占用，orderId=" + message.getOrderId());
            }
            markOrderSuccess(message.getOrderId(), message.getUserId(), message.getVoucherId());
            log.warn("死信对应数据库订单已经存在，已修复 SUCCESS，orderId={}",
                    message.getOrderId());
            return;
        }

        VoucherOrder sameBusinessOrder = query()
                .eq("user_id", message.getUserId())
                .eq("voucher_id", message.getVoucherId())
                .one();
        if (sameBusinessOrder != null) {
            throw new IllegalStateException(
                    "死信用户和优惠券已存在其他订单，拒绝自动补偿，orderId="
                            + message.getOrderId() + "，existingOrderId=" + sameBusinessOrder.getId());
        }

        Long result = stringRedisTemplate.execute(
                ORDER_COMPENSATE_SCRIPT,
                Arrays.asList(
                        SeckillRedisKeys.stockKey(message.getVoucherId()),
                        SeckillRedisKeys.userOrderKey(message.getVoucherId()),
                        SeckillRedisKeys.orderResultKey(message.getOrderId()),
                        ORDER_PENDING_KEY),
                String.valueOf(message.getUserId()),
                String.valueOf(message.getOrderId()),
                String.valueOf(ORDER_FAILED_TTL_SECONDS),
                "DLQ_RETRIES_EXHAUSTED",
                String.valueOf(System.currentTimeMillis()));

        if (result == null) {
            throw new IllegalStateException(
                    "死信补偿 Lua 未返回结果，orderId=" + message.getOrderId());
        }
        int resultCode = result.intValue();
        if (resultCode == 0) {
            log.error("秒杀订单重试耗尽，已恢复 Redis 库存并标记 FAILED，orderId={}",
                    message.getOrderId());
            return;
        }
        if (resultCode == 1) {
            log.warn("秒杀订单死信重复投递，补偿已经完成，orderId={}", message.getOrderId());
            return;
        }

        throw new IllegalStateException(
                "死信订单状态不允许自动补偿，orderId=" + message.getOrderId()
                        + "，compensateResult=" + resultCode);
    }

    /**
     * 正常消费者与 DLQ 消费者必须使用同一个 orderId 锁。
     * 获取失败时抛出异常，让 RocketMQ 稍后重试，不能静默确认消息。
     */
    private void executeWithOrderLock(Long orderId, Runnable action) {
        RLock orderLock = redissonClient.getLock(SeckillRedisKeys.orderLockKey(orderId));
        boolean locked;
        try {
            // 不指定固定 leaseTime，使用 Redisson 看门狗续期，避免数据库事务未结束锁就过期。
            locked = orderLock.tryLock(orderLockWaitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("获取秒杀订单锁被中断，orderId=" + orderId, e);
        }

        if (!locked) {
            throw new IllegalStateException(
                    "秒杀订单正在被其他消费者处理，等待 RocketMQ 重试，orderId=" + orderId);
        }

        try {
            action.run();
        } finally {
            if (orderLock.isHeldByCurrentThread()) {
                orderLock.unlock();
            }
        }
    }

    @Override
    public void reconcileProcessingOrder(Long orderId) {
        String resultKey = SeckillRedisKeys.orderResultKey(orderId);
        Map<Object, Object> result = stringRedisTemplate.opsForHash().entries(resultKey);
        if (result.isEmpty()) {
            log.error("pending 中的订单缺少结果状态，保留待人工核对，orderId={}", orderId);
            return;
        }

        SeckillOrderStatus status = parseOrderStatus(result.get("status"));
        if (status == null) {
            log.error("pending 中的订单状态非法，保留待人工核对，orderId={}，status={}",
                    orderId, result.get("status"));
            return;
        }
        if (status == SeckillOrderStatus.SUCCESS || status == SeckillOrderStatus.FAILED) {
            stringRedisTemplate.opsForZSet().remove(ORDER_PENDING_KEY, String.valueOf(orderId));
            return;
        }

        Long userId = parseLong(result.get("userId"));
        Long voucherId = parseLong(result.get("voucherId"));
        if (userId == null || voucherId == null) {
            log.error("PROCESSING 订单缺少业务字段，保留待人工核对，orderId={}", orderId);
            return;
        }

        VoucherOrder existingOrder = getById(orderId);
        if (existingOrder == null) {
            log.warn("订单长时间处于 PROCESSING，数据库暂不存在订单，等待 MQ/DLQ 处理，orderId={}",
                    orderId);
            return;
        }
        if (!userId.equals(existingOrder.getUserId())
                || !voucherId.equals(existingOrder.getVoucherId())) {
            log.error("PROCESSING 状态与数据库订单归属冲突，保留待人工核对，orderId={}", orderId);
            return;
        }

        markOrderSuccess(orderId, userId, voucherId);
        log.warn("扫描发现数据库订单已存在，已修复 Redis SUCCESS，orderId={}", orderId);
    }

    private void validateMessage(SeckillVoucherMqDTO message) {
        if (message == null
                || message.getOrderId() == null
                || message.getUserId() == null
                || message.getVoucherId() == null) {
            throw new IllegalArgumentException("秒杀订单消息缺少必要字段");
        }
    }

    private boolean isSameOrder(VoucherOrder voucherOrder, SeckillVoucherMqDTO message) {
        return message.getUserId().equals(voucherOrder.getUserId())
                && message.getVoucherId().equals(voucherOrder.getVoucherId());
    }

    /**
     * 只在数据库事务提交后调用。Redis 更新失败时异常继续向消费者传播，
     * RocketMQ 重投后通过订单幂等检查再次补写 SUCCESS。
     */
    private void markOrderSuccess(Long orderId, Long userId, Long voucherId) {
        String resultKey = SeckillRedisKeys.orderResultKey(orderId);
        Long result = stringRedisTemplate.execute(
                ORDER_SUCCESS_SCRIPT,
                Arrays.asList(resultKey, ORDER_PENDING_KEY),
                String.valueOf(orderId), String.valueOf(userId), String.valueOf(voucherId),
                String.valueOf(ORDER_SUCCESS_TTL_SECONDS),
                String.valueOf(System.currentTimeMillis()));
        if (result == null) {
            throw new IllegalStateException("秒杀订单 SUCCESS Lua 未返回结果，orderId=" + orderId);
        }
        if (result.intValue() == 1) {
            throw new IllegalStateException(
                    "订单已经标记 FAILED，禁止覆盖为 SUCCESS，orderId=" + orderId);
        }
        if (result.intValue() != 0) {
            throw new IllegalStateException(
                    "秒杀订单 SUCCESS Lua 返回未知结果，orderId=" + orderId
                            + "，result=" + result);
        }
        log.info("秒杀订单处理状态更新为 SUCCESS，orderId={}", orderId);
    }

    private SeckillOrderStatus getStoredOrderStatus(Long orderId) {
        Object value = stringRedisTemplate.opsForHash().get(
                SeckillRedisKeys.orderResultKey(orderId), "status");
        if (value == null) {
            return null;
        }
        SeckillOrderStatus status = parseOrderStatus(value);
        if (status == null) {
            throw new IllegalStateException(
                    "秒杀订单 Redis 状态非法，orderId=" + orderId + "，status=" + value);
        }
        return status;
    }

    private SeckillOrderStatusDTO buildOrderStatus(
            Long orderId, Long voucherId, SeckillOrderStatus status) {
        return SeckillOrderStatusDTO.builder()
                .orderId(orderId)
                .voucherId(voucherId)
                .status(status)
                .build();
    }

    private Long parseLong(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return Long.valueOf(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private SeckillOrderStatus parseOrderStatus(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return SeckillOrderStatus.valueOf(value.toString());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

}
