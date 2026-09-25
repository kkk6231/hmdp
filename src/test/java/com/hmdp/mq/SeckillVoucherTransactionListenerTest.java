package com.hmdp.mq;

import com.hmdp.constant.MQConstants;
import com.hmdp.constant.SeckillRedisKeys;
import com.hmdp.dto.SeckillVoucherTransactionContext;
import com.hmdp.service.ISeckillVoucherService;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeckillVoucherTransactionListenerTest {

    private final ISeckillVoucherService seckillVoucherService = mock(ISeckillVoucherService.class);
    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
    private final SeckillVoucherTransactionListener listener = new SeckillVoucherTransactionListener();

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(listener, "seckillVoucherService", seckillVoucherService);
        ReflectionTestUtils.setField(listener, "stringRedisTemplate", redisTemplate);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void commitsWhenQualificationIsReserved() {
        SeckillVoucherTransactionContext context =
                new SeckillVoucherTransactionContext(11L, 7L, 10L);
        when(seckillVoucherService.reserveQualification(11L, 7L, 10L)).thenReturn(0L);

        assertEquals(RocketMQLocalTransactionState.COMMIT,
                listener.executeLocalTransaction(message(), context));
        assertEquals(Integer.valueOf(0), context.getLuaResult());
    }

    @Test
    void rollsBackWhenUserAlreadyHasQualification() {
        SeckillVoucherTransactionContext context =
                new SeckillVoucherTransactionContext(11L, 7L, 10L);
        when(seckillVoucherService.reserveQualification(11L, 7L, 10L)).thenReturn(2L);

        assertEquals(RocketMQLocalTransactionState.ROLLBACK,
                listener.executeLocalTransaction(message(), context));
        assertEquals(Integer.valueOf(2), context.getLuaResult());
    }

    @Test
    void transactionCheckRepairsProcessingAfterQualificationWasReserved() {
        when(valueOperations.get(SeckillRedisKeys.transactionKey(11L))).thenReturn(null);
        when(seckillVoucherService.findExistingOrderIdValue(7L, 10L)).thenReturn("11");

        assertEquals(RocketMQLocalTransactionState.COMMIT,
                listener.checkLocalTransaction(message()));
        verify(seckillVoucherService).ensureOrderProcessing(11L, 7L, 10L);
    }

    private Message<String> message() {
        return MessageBuilder.withPayload("seckill-order")
                .setHeader(MQConstants.SECKILL_ORDER_ID_HEADER, "11")
                .setHeader(MQConstants.SECKILL_USER_ID_HEADER, "7")
                .setHeader(MQConstants.SECKILL_VOUCHER_ID_HEADER, "10")
                .build();
    }
}
