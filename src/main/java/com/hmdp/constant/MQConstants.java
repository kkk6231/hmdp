package com.hmdp.constant;

public interface MQConstants {

    String SECKILL_VOUCHER_TOPIC = "SeckillVoucherTopic";

    String SECKILL_VOUCHER_CONSUMER_GROUP = "SeckillVoucherConsumerGroup";

    int SECKILL_VOUCHER_MAX_RECONSUME_TIMES = 3;

    String SECKILL_VOUCHER_DLQ_TOPIC = "%DLQ%" + SECKILL_VOUCHER_CONSUMER_GROUP;

    String SECKILL_VOUCHER_DLQ_CONSUMER_GROUP = "SeckillVoucherDeadLetterConsumerGroup";

    int SECKILL_VOUCHER_DLQ_MAX_RECONSUME_TIMES = 3;

    String SECKILL_ORDER_ID_HEADER = "seckill_order_id";

    String SECKILL_USER_ID_HEADER = "seckill_user_id";

    String SECKILL_VOUCHER_ID_HEADER = "seckill_voucher_id";

}
