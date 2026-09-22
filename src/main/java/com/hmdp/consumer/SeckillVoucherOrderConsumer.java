package com.hmdp.consumer;

import com.hmdp.constant.MQConstants;
import com.hmdp.dto.SeckillVoucherMqDTO;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
@RocketMQMessageListener(
        topic = MQConstants.SECKILL_VOUCHER_TOPIC,
        consumerGroup = MQConstants.SECKILL_VOUCHER_CONSUMER_GROUP,
        maxReconsumeTimes = MQConstants.SECKILL_VOUCHER_MAX_RECONSUME_TIMES
)
public class SeckillVoucherOrderConsumer implements RocketMQListener<SeckillVoucherMqDTO> {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Override
    public void onMessage(SeckillVoucherMqDTO message) {
        log.info("收到秒杀订单消息，message={}", message);
        voucherOrderService.createVoucherOrder(message);
    }
}
