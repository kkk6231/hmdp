package com.hmdp.constant;

/**
 * 秒杀模块对外返回文案。
 *
 * <p>这里只保存用户可见的稳定提示语，日志与内部异常信息不放在这里。</p>
 */
public final class SeckillResultMessages {

    public static final String SYSTEM_BUSY_RETRY_LATER = "系统繁忙，请稍后重试";
    public static final String SYSTEM_BUSY_RETRY = "系统繁忙，请重试";
    public static final String OUT_OF_STOCK = "库存不足";
    public static final String ACTIVITY_NOT_STARTED = "秒杀活动尚未开始";
    public static final String ACTIVITY_ENDED = "秒杀活动已结束";
    public static final String ACTIVITY_NOT_READY = "秒杀活动尚未预热，请稍后重试";
    public static final String ACTIVITY_INFO_INVALID = "秒杀活动信息异常，请稍后重试";
    public static final String SECKILL_SERVICE_ERROR = "秒杀服务异常，请稍后重试";
    public static final String VOUCHER_ID_REQUIRED = "优惠券不能为空";
    public static final String ORDER_ID_REQUIRED = "订单号不能为空";
    public static final String ORDER_ACCESS_DENIED = "无权查询该订单";
    public static final String ORDER_STATUS_UNAVAILABLE = "订单状态暂时不可用，请稍后重试";
    public static final String ORDER_NOT_FOUND_OR_EXPIRED = "订单不存在或处理结果已过期";

    private SeckillResultMessages() {
    }
}
