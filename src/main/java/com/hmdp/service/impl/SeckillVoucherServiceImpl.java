package com.hmdp.service.impl;

import com.hmdp.constant.SeckillRedisKeys;
import com.hmdp.constant.SeckillResultMessages;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.hmdp.constant.SeckillRedisKeys.MQ_TRANSACTION_TTL_SECONDS;
import static com.hmdp.constant.SeckillRedisKeys.ORDER_PENDING_KEY;
import static com.hmdp.constant.SeckillRedisKeys.VOUCHER_KEY_GRACE_SECONDS;

/**
 * 秒杀券侧业务实现。Redis 中保存活动准入与预扣状态，MySQL 库存扣减由订单服务
 * 放在创建订单的数据库事务中调用，避免只扣库存而没有订单。
 */
@Slf4j
@Service
public class SeckillVoucherServiceImpl extends ServiceImpl<SeckillVoucherMapper, SeckillVoucher> implements ISeckillVoucherService {

    /** 一次 Lua 执行完成活动校验、资格判重、预扣库存和结果状态写入。 */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    /** 事务回查时只补写 PROCESSING，不覆盖 SUCCESS 或 FAILED。 */
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
     * 本地时间检查只用于快速拒绝无效请求；发送 Half Message 后，
     * 资格 Lua 会按 Redis 服务器时间再次检查活动边界。
     */
    @Override
    public Result validateActivityTime(Long voucherId) {
        if (voucherId == null) {
            return Result.fail(SeckillResultMessages.VOUCHER_ID_REQUIRED);
        }

        try {
            // 活动元数据缺失表示尚未预热，不能让请求继续进入 MQ 链路。
            Map<Object, Object> activityMeta = stringRedisTemplate.opsForHash().entries(
                    SeckillRedisKeys.voucherMetaKey(voucherId));
            if (activityMeta.isEmpty()) {
                return Result.fail(SeckillResultMessages.ACTIVITY_NOT_READY);
            }

            Long beginTime = parseLong(
                    activityMeta.get(SeckillRedisKeys.VOUCHER_BEGIN_TIME_FIELD));
            Long endTime = parseLong(
                    activityMeta.get(SeckillRedisKeys.VOUCHER_END_TIME_FIELD));
            if (beginTime == null || endTime == null || beginTime >= endTime) {
                log.error("秒杀活动时间元数据非法，voucherId={}，beginTime={}，endTime={}",
                        voucherId, beginTime, endTime);
                return Result.fail(SeckillResultMessages.ACTIVITY_INFO_INVALID);
            }

            // 这里的应用服务器时间仅用于预过滤，不作为最终的秒杀准入依据。
            long now = System.currentTimeMillis();
            if (now < beginTime) {
                return Result.fail(SeckillResultMessages.ACTIVITY_NOT_STARTED);
            }
            if (now >= endTime) {
                return Result.fail(SeckillResultMessages.ACTIVITY_ENDED);
            }
            return null;
        } catch (Exception e) {
            log.error("读取秒杀活动时间元数据失败，voucherId={}", voucherId, e);
            return Result.fail(SeckillResultMessages.SYSTEM_BUSY_RETRY_LATER);
        }
    }

    /**
     * 保留原始字符串：订单服务会校验它是否为有效订单号；事务回查则需要
     * 区分“映射不存在”和“映射存在但不是当前订单”。
     */
    @Override
    public String findExistingOrderIdValue(Long userId, Long voucherId) {
        Object value = stringRedisTemplate.opsForHash().get(
                SeckillRedisKeys.userOrderKey(voucherId), String.valueOf(userId));
        return value == null ? null : value.toString();
    }

    /**
     * 获取秒杀资格的唯一写入口。KEYS 顺序必须与 seckill.lua 一致，
     * 返回码交由事务监听器决定提交、回滚或保持 UNKNOWN。
     */
    @Override
    public Long reserveQualification(Long orderId, Long userId, Long voucherId) {
        return stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Arrays.asList(
                        SeckillRedisKeys.stockKey(voucherId),
                        SeckillRedisKeys.userOrderKey(voucherId),
                        SeckillRedisKeys.transactionKey(orderId),
                        SeckillRedisKeys.orderResultKey(orderId),
                        ORDER_PENDING_KEY,
                        SeckillRedisKeys.voucherMetaKey(voucherId)),
                String.valueOf(userId), String.valueOf(orderId), String.valueOf(voucherId),
                String.valueOf(MQ_TRANSACTION_TTL_SECONDS),
                String.valueOf(System.currentTimeMillis()));
    }

    /** 补齐事务消息已确认提交、但订单处理状态可能未写完整的 Redis 数据。 */
    @Override
    public void ensureOrderProcessing(Long orderId, Long userId, Long voucherId) {
        stringRedisTemplate.execute(
                ORDER_PROCESSING_SCRIPT,
                Arrays.asList(SeckillRedisKeys.orderResultKey(orderId), ORDER_PENDING_KEY),
                String.valueOf(orderId), String.valueOf(userId), String.valueOf(voucherId),
                String.valueOf(System.currentTimeMillis()));
    }

    /**
     * 仅当数据库库存大于零时扣减一件；返回 false 由订单服务抛异常，
     * 使库存扣减与订单插入一起回滚。
     */
    @Override
    public boolean decreaseStock(Long voucherId) {
        return update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
    }

    /**
     * 预热 Redis 库存和活动时间。两个 Key 在活动结束后额外保留恢复窗口，
     * 供已获资格但仍在消费或补偿中的订单使用。
     */
    @Override
    public void preheatSeckillVoucher(SeckillVoucher voucher) {
        Long voucherId = voucher.getVoucherId();
        String stockKey = SeckillRedisKeys.stockKey(voucherId);
        String metaKey = SeckillRedisKeys.voucherMetaKey(voucherId);
        stringRedisTemplate.opsForValue().set(stockKey, voucher.getStock().toString());

        Map<String, String> activityMeta = new HashMap<>();
        activityMeta.put(
                SeckillRedisKeys.VOUCHER_BEGIN_TIME_FIELD,
                String.valueOf(voucher.getBeginTime()
                        .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        activityMeta.put(
                SeckillRedisKeys.VOUCHER_END_TIME_FIELD,
                String.valueOf(voucher.getEndTime()
                        .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        stringRedisTemplate.opsForHash().putAll(metaKey, activityMeta);

        // 库存和元数据一起过期，避免资格脚本只读到其中一部分。
        Date expireAt = Date.from(voucher.getEndTime()
                .plusSeconds(VOUCHER_KEY_GRACE_SECONDS)
                .atZone(ZoneId.systemDefault())
                .toInstant());
        stringRedisTemplate.expireAt(stockKey, expireAt);
        stringRedisTemplate.expireAt(metaKey, expireAt);
    }

    /** Redis 元数据格式异常时返回 null，由调用方给出活动信息异常提示。 */
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
}
