package com.hmdp.controller;


import com.hmdp.annotation.SeckillRateLimit;
import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    /**
     * 优惠券秒杀
     * @param voucherId
     * @return
     */
    @PostMapping("seckill/{id}")
    @SeckillRateLimit(
            userMaxCount = 5,
            userWindowSeconds = 10,
            ipMaxCount = 50,
            ipWindowSeconds = 10
    )
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    /**
     * 查询秒杀订单的异步处理状态。
     */
    @GetMapping("status/{orderId}")
    public Result querySeckillOrderStatus(@PathVariable Long orderId) {
        return voucherOrderService.querySeckillOrderStatus(orderId);
    }
}
