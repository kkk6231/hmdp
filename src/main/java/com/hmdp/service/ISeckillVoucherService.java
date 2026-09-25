package com.hmdp.service;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.dto.Result;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * 秒杀券侧的业务能力：活动校验、Redis 资格预留、库存扣减和活动预热。
 * 下单消息发送及订单持久化由订单服务负责。
 */
public interface ISeckillVoucherService extends IService<SeckillVoucher> {

    /**
     * 在发送事务消息前快速检查活动时间，减少无效 Half Message。
     * 校验通过返回 null；最终准入仍由 Lua 使用 Redis 时间判断。
     */
    Result validateActivityTime(Long voucherId);

    /** 返回 Redis 中的原始订单号，供重复请求和事务回查分别判断。 */
    String findExistingOrderIdValue(Long userId, Long voucherId);

    /** 原子校验时间、一人一单和库存，并保存事务状态与 PROCESSING 结果。 */
    Long reserveQualification(Long orderId, Long userId, Long voucherId);

    /** Broker 回查确认已预留资格时，补齐可能缺失的 PROCESSING 状态。 */
    void ensureOrderProcessing(Long orderId, Long userId, Long voucherId);

    /** 条件扣减数据库库存；由订单服务在插入订单的同一事务中调用。 */
    boolean decreaseStock(Long voucherId);

    /** 将新建秒杀券的库存和活动时间写入 Redis，并设置恢复窗口。 */
    void preheatSeckillVoucher(SeckillVoucher voucher);
}
