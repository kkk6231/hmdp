package com.hmdp.consumer;

import com.hmdp.constant.MQConstants;
import com.hmdp.dto.SeckillVoucherMqDTO;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 秒杀订单死信消费者。
 *
 * <p>默认关闭，完成真实故障测试后再开启。处理成功前不会吞掉死信。</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(
        prefix = "hmdp.mq",
        name = "dlq-consumer-enabled",
        havingValue = "true"
)
@RocketMQMessageListener(
        topic = MQConstants.SECKILL_VOUCHER_DLQ_TOPIC,
        consumerGroup = MQConstants.SECKILL_VOUCHER_DLQ_CONSUMER_GROUP,
        maxReconsumeTimes = MQConstants.SECKILL_VOUCHER_DLQ_MAX_RECONSUME_TIMES
)
public class SeckillVoucherDeadLetterConsumer implements RocketMQListener<SeckillVoucherMqDTO> {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Override
    public void onMessage(SeckillVoucherMqDTO message) {
        log.error("收到秒杀订单死信，开始核对数据库并恢复，message={}", message);
        voucherOrderService.handleDeadLetter(message);
    }
}
