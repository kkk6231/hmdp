package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillVoucherMqDTO;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    Result querySeckillOrderStatus(Long orderId);

    void createVoucherOrder(SeckillVoucherMqDTO message);

    void handleDeadLetter(SeckillVoucherMqDTO message);
}
