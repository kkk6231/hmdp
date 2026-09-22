package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import com.hmdp.constant.SeckillRedisKeys;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hmdp.constant.SeckillRedisKeys.VOUCHER_KEY_GRACE_SECONDS;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryVoucherOfShop(Long shopId) {
        // 查询优惠券信息
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        // 返回结果
        return Result.ok(vouchers);
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        if (voucher.getBeginTime() == null || voucher.getEndTime() == null) {
            throw new IllegalArgumentException("秒杀活动开始时间和结束时间不能为空");
        }
        if (!voucher.getBeginTime().isBefore(voucher.getEndTime())) {
            throw new IllegalArgumentException("秒杀活动开始时间必须早于结束时间");
        }
        // 保存优惠券
        save(voucher);
        // 保存秒杀信息
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        seckillVoucherService.save(seckillVoucher);
        // 保存秒杀库存和活动时间元数据到 Redis 中。
        // 活动结束后仍保留恢复窗口，供已获得资格的订单完成消费或库存补偿。
        String stockKey = SeckillRedisKeys.stockKey(voucher.getId());
        String metaKey = SeckillRedisKeys.voucherMetaKey(voucher.getId());
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

        Date expireAt = Date.from(voucher.getEndTime()
                .plusSeconds(VOUCHER_KEY_GRACE_SECONDS)
                .atZone(ZoneId.systemDefault())
                .toInstant());
        stringRedisTemplate.expireAt(stockKey, expireAt);
        stringRedisTemplate.expireAt(metaKey, expireAt);
    }
}
