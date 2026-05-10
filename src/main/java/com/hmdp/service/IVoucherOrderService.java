package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    /**
     * 秒杀优惠券
     * @param voucherId 优惠券ID
     * @return 结果
     */
    Result seckillVoucher(Long voucherId);

    /**
     * 创建购买优惠券的订单
     * @param voucherId
     * @param userId
     * @return
     */
    Result createVoucherOrder(Long voucherId, Long userId);
}

