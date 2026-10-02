package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

/**
 * 优惠券基础信息服务。新增秒杀券时负责协调基础券与秒杀券两张表，
 * 秒杀券的 Redis 预热细节由秒杀券服务实现。
 */
@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    /** 根据店铺查询可展示的优惠券列表。 */
    @Override
    public Result queryVoucherOfShop(Long shopId) {
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        return Result.ok(vouchers);
    }

    /** 保存基础券和秒杀信息，并预热 Redis。 */
    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        if (voucher.getBeginTime() == null || voucher.getEndTime() == null) {
            throw new IllegalArgumentException("秒杀活动开始时间和结束时间不能为空");
        }
        if (!voucher.getBeginTime().isBefore(voucher.getEndTime())) {
            throw new IllegalArgumentException("秒杀活动开始时间必须早于结束时间");
        }
        save(voucher);
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        seckillVoucherService.save(seckillVoucher);
        // 活动结束后仍保留恢复窗口，供已获得资格的订单完成消费或库存补偿。
        seckillVoucherService.preheatSeckillVoucher(seckillVoucher);
    }
}
