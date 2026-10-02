package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.constant.MQConstants;
import com.hmdp.constant.SeckillLuaResultCode;
import com.hmdp.constant.SeckillOrderStatus;
import com.hmdp.constant.SeckillRedisKeys;
import com.hmdp.constant.SeckillResultMessages;
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
import org.apache.rocketmq.client.producer.LocalTransactionState;
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

/**
 * 秒杀下单与订单结果服务。请求线程负责发送事务消息，不直接写订单；
 * 消费线程在数据库事务内扣库存并建单，失败后的状态修复由重试、死信和扫描入口处理。
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

    /** 发起秒杀并返回资格对应的订单号，数据库订单由消费者异步创建。 */
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();

        // 此处仅快速拒绝无效请求，Lua 会使用 Redis 时间做最终校验。
        Result activityTimeResult = seckillVoucherService.validateActivityTime(voucherId);
        if (activityTimeResult != null) {
            return activityTimeResult;
        }

        // 消息、Redis 资格和数据库订单共用同一个订单号。
        long orderId = redisIdWorker.nextId("order");

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

        // 本地事务上下文只在发送线程和回调之间共享；Broker 回查依赖消息 Header。
        SeckillVoucherTransactionContext context =
                new SeckillVoucherTransactionContext(orderId, userId, voucherId);

        try {
            TransactionSendResult sendResult = rocketMQTemplate.sendMessageInTransaction(
                    MQConstants.SECKILL_VOUCHER_TOPIC,
                    message,
                    context
            );

            // 发送状态异常不代表 Redis Lua 一定没执行，不能在这里回补库存。
            if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
                log.error("秒杀事务消息发送状态异常，orderId={}，sendStatus={}，localState={}",
                        orderId,
                        sendResult == null ? null : sendResult.getSendStatus(),
                        sendResult == null ? null : sendResult.getLocalTransactionState());
                return Result.fail(SeckillResultMessages.SYSTEM_BUSY_RETRY_LATER);
            }
            if (sendResult.getLocalTransactionState() == LocalTransactionState.UNKNOW) {
                return buildUnknownSeckillResult(orderId);
            }
        } catch (MessagingException e) {
            log.error("秒杀事务消息发送失败，orderId={}，userId={}，voucherId={}",
                    orderId, userId, voucherId, e);
            // 已取得明确的业务拒绝码时可直接返回；其余情况保留未知语义供重试。
            if (context.getLuaResult() != null
                    && context.getLuaResult() != SeckillLuaResultCode.SUCCESS) {
                return resolveSeckillResult(orderId, userId, voucherId, context.getLuaResult());
            }
            return buildUnknownSeckillResult(orderId);
        }

        return resolveSeckillResult(orderId, userId, voucherId, context.getLuaResult());
    }

    /** 将 Lua 返回码转换成接口结果；重复请求返回原订单号。 */
    private Result resolveSeckillResult(
            long orderId, Long userId, Long voucherId, Integer luaResult) {
        if (luaResult == null) {
            return buildUnknownSeckillResult(orderId);
        }
        if (luaResult == SeckillLuaResultCode.OUT_OF_STOCK) {
            return Result.fail(SeckillResultMessages.OUT_OF_STOCK);
        }
        if (luaResult == SeckillLuaResultCode.DUPLICATE_ORDER) {
            return resolveExistingSeckillQualification(userId, voucherId);
        }
        if (luaResult == SeckillLuaResultCode.ACTIVITY_NOT_STARTED) {
            return Result.fail(SeckillResultMessages.ACTIVITY_NOT_STARTED);
        }
        if (luaResult == SeckillLuaResultCode.ACTIVITY_ENDED) {
            return Result.fail(SeckillResultMessages.ACTIVITY_ENDED);
        }
        if (luaResult == SeckillLuaResultCode.ACTIVITY_NOT_READY) {
            return Result.fail(SeckillResultMessages.ACTIVITY_NOT_READY);
        }
        if (luaResult != SeckillLuaResultCode.SUCCESS) {
            log.error("秒杀 Lua 返回未知结果，orderId={}，result={}", orderId, luaResult);
            return Result.fail(SeckillResultMessages.SECKILL_SERVICE_ERROR);
        }
        return Result.ok(orderId);
    }

    /** UNKNOWN 时不宣称抢购成功，Broker 仍会继续回查。 */
    private Result buildUnknownSeckillResult(long orderId) {
        log.warn("秒杀本地事务状态未知，提示用户重试，orderId={}", orderId);
        return Result.fail(SeckillResultMessages.SYSTEM_BUSY_RETRY);
    }

    /** 重复请求返回首次获得资格时生成的订单号。 */
    private Result resolveExistingSeckillQualification(Long userId, Long voucherId) {
        try {
            Long existingOrderId = parseLong(
                    seckillVoucherService.findExistingOrderIdValue(userId, voucherId));
            if (existingOrderId == null || existingOrderId <= 0) {
                log.warn("Lua 返回重复下单，但未查询到有效的原订单号，userId={}，voucherId={}",
                        userId, voucherId);
                return Result.fail(SeckillResultMessages.SYSTEM_BUSY_RETRY_LATER);
            }

            log.info("用户重复请求命中已有秒杀资格，返回原订单号，userId={}，voucherId={}，orderId={}",
                    userId, voucherId, existingOrderId);
            return Result.ok(existingOrderId);
        } catch (Exception e) {
            log.error("查询用户已有秒杀资格失败，userId={}，voucherId={}",
                    userId, voucherId, e);
            return Result.fail(SeckillResultMessages.SYSTEM_BUSY_RETRY_LATER);
        }
    }

    /** 查询异步订单状态；Redis 无记录时以数据库订单兜底。 */
    @Override
    public Result querySeckillOrderStatus(Long orderId) {
        if (orderId == null) {
            return Result.fail(SeckillResultMessages.ORDER_ID_REQUIRED);
        }

        Long currentUserId = UserHolder.getUser().getId();
        String stateKey = SeckillRedisKeys.orderStateKey(orderId);
        boolean redisAvailable = true;
        try {
            Map<Object, Object> state = stringRedisTemplate.opsForHash().entries(stateKey);
            if (!state.isEmpty()) {
                // 状态记录携带用户 ID，不能只凭订单号向其他用户泄露处理状态。
                Long resultUserId = parseLong(state.get("userId"));
                if (resultUserId == null || !currentUserId.equals(resultUserId)) {
                    return Result.fail(SeckillResultMessages.ORDER_ACCESS_DENIED);
                }

                SeckillOrderStatus status = parseOrderStatus(state.get("status"));
                Long voucherId = parseLong(state.get("voucherId"));
                if (status != null) {
                    return Result.ok(buildOrderStatus(orderId, voucherId, status));
                }
            }
        } catch (Exception e) {
            redisAvailable = false;
            log.warn("查询 Redis 秒杀订单状态失败，回退查询数据库，orderId={}", orderId, e);
        }

        // Redis 无结果时，数据库订单是最终成功事实。
        VoucherOrder voucherOrder = getById(orderId);
        if (voucherOrder != null) {
            if (!currentUserId.equals(voucherOrder.getUserId())) {
                return Result.fail(SeckillResultMessages.ORDER_ACCESS_DENIED);
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
            return Result.fail(SeckillResultMessages.ORDER_STATUS_UNAVAILABLE);
        }
        return Result.fail(SeckillResultMessages.ORDER_NOT_FOUND_OR_EXPIRED);
    }

    /** 消费订单消息，并按订单号加锁避免与死信补偿并发。 */
    @Override
    public void createVoucherOrder(SeckillVoucherMqDTO message) {
        validateMessage(message);
        executeWithOrderLock(
                message.getOrderId(),
                () -> doCreateVoucherOrder(message));
    }

    /** 幂等建单；数据库提交后再写入 Redis 成功状态。 */
    private void doCreateVoucherOrder(SeckillVoucherMqDTO message) {
        SeckillOrderStatus redisStatus = getStoredOrderStatus(message.getOrderId());

        // 消费成功但 ACK 失败时，消息可能被重复投递。
        VoucherOrder existingOrder = getById(message.getOrderId());
        if (existingOrder != null) {
            if (isSameOrder(existingOrder, message)) {
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

        // 已补偿的消息即使迟到，也不能再次扣减数据库库存。
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
            // 事务模板保证数据库扣库存与订单插入同成同败。
            Boolean created = transactionTemplate.execute(status -> {
                boolean stockUpdated = seckillVoucherService.decreaseStock(message.getVoucherId());
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

            log.info("秒杀订单创建成功，orderId={}，userId={}，voucherId={}",
                    message.getOrderId(), message.getUserId(), message.getVoucherId());
            markOrderSuccess(message.getOrderId(), message.getUserId(), message.getVoucherId());
        } catch (DuplicateKeyException e) {
            // 并发消费者可能同时通过前置查询，最终由数据库唯一键兜底。
            VoucherOrder duplicateOrder = getById(message.getOrderId());
            if (duplicateOrder != null && isSameOrder(duplicateOrder, message)) {
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

    /** 处理死信消息，与正常消费共用订单锁。 */
    @Override
    public void handleDeadLetter(SeckillVoucherMqDTO message) {
        validateMessage(message);

        executeWithOrderLock(
                message.getOrderId(),
                () -> doHandleDeadLetter(message));
    }

    /** 以数据库为准判断修复成功状态或回补 Redis 资格。 */
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

        // 已有另一订单号时不能释放用户资格，否则可能让同一用户再次抢购。
        VoucherOrder sameBusinessOrder = query()
                .eq("user_id", message.getUserId())
                .eq("voucher_id", message.getVoucherId())
                .one();
        if (sameBusinessOrder != null) {
            throw new IllegalStateException(
                    "死信用户和优惠券已存在其他订单，拒绝自动补偿，orderId="
                            + message.getOrderId() + "，existingOrderId=" + sameBusinessOrder.getId());
        }

        // Lua 会核对 PROCESSING 状态和 userId -> orderId 映射，保证重复死信只回补一次。
        Long result = stringRedisTemplate.execute(
                ORDER_COMPENSATE_SCRIPT,
                Arrays.asList(
                        SeckillRedisKeys.stockKey(message.getVoucherId()),
                        SeckillRedisKeys.userOrderKey(
                                message.getVoucherId(), message.getUserId()),
                        SeckillRedisKeys.orderStateKey(message.getOrderId())),
                String.valueOf(message.getOrderId()),
                String.valueOf(SeckillRedisKeys.ORDER_FAILED_TTL_SECONDS),
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

    /** 使用统一订单锁串行化正常消费与死信补偿。 */
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

    /** 拒绝缺少订单号、用户号或券号的消息，交由 MQ 重试或进入死信处理。 */
    private void validateMessage(SeckillVoucherMqDTO message) {
        if (message == null
                || message.getOrderId() == null
                || message.getUserId() == null
                || message.getVoucherId() == null) {
            throw new IllegalArgumentException("秒杀订单消息缺少必要字段");
        }
    }

    /** 同一 orderId 只有业务字段也一致时，才视为安全的重复投递。 */
    private boolean isSameOrder(VoucherOrder voucherOrder, SeckillVoucherMqDTO message) {
        return message.getUserId().equals(voucherOrder.getUserId())
                && message.getVoucherId().equals(voucherOrder.getVoucherId());
    }

    /** 数据库提交后写成功状态；失败时由 MQ 重投后再次补写。 */
    private void markOrderSuccess(Long orderId, Long userId, Long voucherId) {
        String stateKey = SeckillRedisKeys.orderStateKey(orderId);
        Long result = stringRedisTemplate.execute(
                ORDER_SUCCESS_SCRIPT,
                Arrays.asList(stateKey),
                String.valueOf(userId), String.valueOf(voucherId),
                String.valueOf(SeckillRedisKeys.ORDER_SUCCESS_TTL_SECONDS),
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

    /** 读取 Redis 订单终态；非法状态抛异常，避免把未知状态当新订单处理。 */
    private SeckillOrderStatus getStoredOrderStatus(Long orderId) {
        Object value = stringRedisTemplate.opsForHash().get(
                SeckillRedisKeys.orderStateKey(orderId), "status");
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

    /** 统一组装状态查询接口的返回数据。 */
    private SeckillOrderStatusDTO buildOrderStatus(
            Long orderId, Long voucherId, SeckillOrderStatus status) {
        return SeckillOrderStatusDTO.builder()
                .orderId(orderId)
                .voucherId(voucherId)
                .status(status)
                .build();
    }

    /** Redis 字段缺失或格式错误时返回 null，由各业务分支决定如何处理。 */
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

    /** Redis 状态不是已定义枚举值时返回 null，避免误认成成功。 */
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
