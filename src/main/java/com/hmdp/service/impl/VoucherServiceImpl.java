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
        // 查询优惠券信息
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        // 返回结果
        return Result.ok(vouchers);
    }

    /**
     * 新增秒杀券：先保存基础券取得 ID，再保存关联的秒杀券记录，最后预热 Redis。
     * 两次数据库写入处于同一事务；当前 Redis 预热发生在该事务提交之前。
     */
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
        // 活动结束后仍保留恢复窗口，供已获得资格的订单完成消费或库存补偿。
        seckillVoucherService.preheatSeckillVoucher(seckillVoucher);
    }
}
