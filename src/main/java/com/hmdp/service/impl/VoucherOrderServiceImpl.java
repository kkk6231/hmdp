package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.ILock;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    /**
     * 优惠券秒杀
     * @param voucherId 优惠券ID
     * @return
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1. 查秒杀券信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher == null) {
            return Result.fail("优惠券不存在");
        }

        // 2. 检查是否在有效期内
        if (LocalDateTime.now().isBefore(voucher.getBeginTime())) {
            return Result.fail("活动尚未开始");
        }
        if (LocalDateTime.now().isAfter(voucher.getEndTime())) {
            return Result.fail("活动已结束");
        }

        // 3. 检查库存
        if (voucher.getStock() < 1) {
            return Result.fail("秒杀券已被抢光！");
        }

        // 4. 一人一单
        Long userId = UserHolder.getUser().getId();
        // 使用分布式锁
        //ILock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        //boolean isLocked = lock.tryLock(1200);
        // 使用Redission的分布式锁
        RLock lock = redissonClient.getLock("order:" + userId);
        boolean isLocked = lock.tryLock();
        // 判断获取锁是否成功
        if (!isLocked) {
            // 获取锁失败
            return Result.fail("系统繁忙，请稍后再试");
        }
        // 若获取锁成功，进行后续操作
        try {
            // 获取代理对象（spring的事务是基于代理对象的）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            // 代理对象调用事务管理的方法
            return proxy.createVoucherOrder(voucherId, userId);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    /**
     * 创建优惠券的订单
     * @param voucherId
     * @param userId
     * @return
     */
    @Transactional
    public Result createVoucherOrder(Long voucherId, Long userId) {
        // 再次检查库存（防止并发问题）
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }

        // 检查是否已经下过单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            return Result.fail("您已抢购过该优惠券");
        }

        // 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")  //sql语句
                .eq("voucher_id", voucherId)
                .gt("stock", 0) //乐观锁解决库存超卖
                .update();
        if (!success) {
            return Result.fail("库存不足");
        }

        // 创建订单
        VoucherOrder order = new VoucherOrder();
        // 用户id
        order.setUserId(userId);
        // 优惠券id
        order.setVoucherId(voucherId);
        // 订单id
        long orderId = redisIdWorker.nextId("order");
        order.setId(orderId);
        save(order);

        return Result.ok(order.getId());
    }

}