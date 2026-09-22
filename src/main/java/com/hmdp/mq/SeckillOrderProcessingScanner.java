package com.hmdp.mq;

import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;

import static com.hmdp.constant.SeckillRedisKeys.ORDER_PENDING_KEY;

/**
 * 长期 PROCESSING 扫描器。
 *
 * <p>只允许根据数据库事实修复 SUCCESS 或输出告警，绝不在这里恢复库存。</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(
        prefix = "hmdp.mq",
        name = "processing-scan-enabled",
        havingValue = "true"
)
public class SeckillOrderProcessingScanner {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IVoucherOrderService voucherOrderService;

    @Value("${hmdp.mq.processing-timeout-seconds:300}")
    private long processingTimeoutSeconds;

    @Value("${hmdp.mq.processing-scan-batch-size:100}")
    private long scanBatchSize;

    @Scheduled(fixedDelayString = "${hmdp.mq.processing-scan-interval-ms:60000}")
    public void scan() {
        long cutoffTimestamp = System.currentTimeMillis() - processingTimeoutSeconds * 1000L;
        Set<String> orderIds = stringRedisTemplate.opsForZSet().rangeByScore(
                ORDER_PENDING_KEY, 0, cutoffTimestamp, 0, scanBatchSize);
        if (orderIds == null || orderIds.isEmpty()) {
            return;
        }

        for (String orderIdValue : orderIds) {
            try {
                voucherOrderService.reconcileProcessingOrder(Long.valueOf(orderIdValue));
            } catch (Exception e) {
                log.error("扫描 PROCESSING 订单失败，保留 pending 等待下次处理，orderId={}",
                        orderIdValue, e);
            }
        }
    }
}
