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

    /** 快速校验活动时间；最终准入由 Lua 使用 Redis 时间判断。 */
    @Override
    public Result validateActivityTime(Long voucherId) {
        if (voucherId == null) {
            return Result.fail(SeckillResultMessages.VOUCHER_ID_REQUIRED);
        }

        try {
            // 活动元数据缺失表示尚未预热，不能让请求继续进入 MQ 链路。
            Map<Object, Object> activityMeta = stringRedisTemplate.opsForHash().entries(
                    SeckillRedisKeys.voucherTimeKey(voucherId));
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

    /** 返回用户资格绑定的原始订单号，用于重复请求和事务回查。 */
    @Override
    public String findExistingOrderIdValue(Long userId, Long voucherId) {
        return stringRedisTemplate.opsForValue().get(
                SeckillRedisKeys.userOrderKey(voucherId, userId));
    }

    /** 原子预留秒杀资格，返回码由事务监听器转换为消息事务状态。 */
    @Override
    public Long reserveSeckillOrder(Long orderId, Long userId, Long voucherId) {
        return stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Arrays.asList(
                        SeckillRedisKeys.stockKey(voucherId),
                        SeckillRedisKeys.userOrderKey(voucherId, userId),
                        SeckillRedisKeys.transactionKey(orderId),
                        SeckillRedisKeys.orderStateKey(orderId),
                        SeckillRedisKeys.voucherTimeKey(voucherId)),
                String.valueOf(userId), String.valueOf(orderId), String.valueOf(voucherId),
                String.valueOf(MQ_TRANSACTION_TTL_SECONDS),
                String.valueOf(System.currentTimeMillis()));
    }

    /** 补齐事务消息已确认提交、但订单处理状态可能未写完整的 Redis 数据。 */
    @Override
    public void ensureOrderProcessing(Long orderId, Long userId, Long voucherId) {
        stringRedisTemplate.execute(
                ORDER_PROCESSING_SCRIPT,
                Arrays.asList(SeckillRedisKeys.orderStateKey(orderId)),
                String.valueOf(userId), String.valueOf(voucherId),
                String.valueOf(System.currentTimeMillis()));
    }

    /** 仅在库存充足时扣减，调用方负责将扣库存与建单放在同一事务。 */
    @Override
    public boolean decreaseStock(Long voucherId) {
        return update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
    }

    /** 预热库存和活动时间，并在活动结束后保留补偿窗口。 */
    @Override
    public void preheatSeckillVoucher(SeckillVoucher voucher) {
        Long voucherId = voucher.getVoucherId();
        String stockKey = SeckillRedisKeys.stockKey(voucherId);
        String timeKey = SeckillRedisKeys.voucherTimeKey(voucherId);
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
        stringRedisTemplate.opsForHash().putAll(timeKey, activityMeta);

        // 库存和元数据一起过期，避免资格脚本只读到其中一部分。
        Date expireAt = Date.from(voucher.getEndTime()
                .plusSeconds(VOUCHER_KEY_GRACE_SECONDS)
                .atZone(ZoneId.systemDefault())
                .toInstant());
        stringRedisTemplate.expireAt(stockKey, expireAt);
        stringRedisTemplate.expireAt(timeKey, expireAt);
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
