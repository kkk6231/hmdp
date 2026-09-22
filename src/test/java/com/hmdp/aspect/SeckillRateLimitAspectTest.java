package com.hmdp.aspect;

import com.hmdp.annotation.SeckillRateLimit;
import com.hmdp.constant.SeckillRateLimitResult;
import com.hmdp.constant.SeckillRedisKeys;
import com.hmdp.constant.SeckillResultMessages;
import com.hmdp.dto.UserDTO;
import com.hmdp.exception.RateLimitException;
import com.hmdp.service.IRateLimitService;
import com.hmdp.utils.UserHolder;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeckillRateLimitAspectTest {

    private final IRateLimitService rateLimitService = mock(IRateLimitService.class);
    private final ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
    private final SeckillRateLimitAspect aspect = new SeckillRateLimitAspect();
    private SeckillRateLimit rule;

    @BeforeEach
    void setUp() throws Exception {
        injectRateLimitService();
        Method method = TestController.class.getDeclaredMethod("seckill", Long.class);
        rule = method.getAnnotation(SeckillRateLimit.class);

        UserDTO user = new UserDTO();
        user.setId(7L);
        UserHolder.saveUser(user);
        when(joinPoint.getArgs()).thenReturn(new Object[]{10L});
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void shouldProceedWhenBothLimitsPass() throws Throwable {
        String userKey = SeckillRedisKeys.rateLimitUserKey(10L, 7L);
        String voucherKey = SeckillRedisKeys.rateLimitVoucherKey(10L);
        Object expected = new Object();

        whenTryAcquire(userKey, voucherKey, SeckillRateLimitResult.PASS);
        when(joinPoint.proceed()).thenReturn(expected);

        Object actual = aspect.doAround(joinPoint, rule);

        assertSame(expected, actual);
        verify(joinPoint).proceed();
    }

    @Test
    void shouldRejectWithUserMessageWhenUserLimitIsExceeded() {
        String userKey = SeckillRedisKeys.rateLimitUserKey(10L, 7L);
        String voucherKey = SeckillRedisKeys.rateLimitVoucherKey(10L);
        whenTryAcquire(userKey, voucherKey, SeckillRateLimitResult.USER_LIMITED);

        RateLimitException exception = assertThrows(
                RateLimitException.class,
                () -> aspect.doAround(joinPoint, rule));

        assertEquals(SeckillResultMessages.USER_REQUEST_TOO_FREQUENT, exception.getMessage());
        verifyBusinessNeverRuns();
    }

    @Test
    void shouldRejectWithBusyMessageWhenVoucherLimitIsExceeded() {
        String userKey = SeckillRedisKeys.rateLimitUserKey(10L, 7L);
        String voucherKey = SeckillRedisKeys.rateLimitVoucherKey(10L);
        whenTryAcquire(userKey, voucherKey, SeckillRateLimitResult.VOUCHER_LIMITED);

        RateLimitException exception = assertThrows(
                RateLimitException.class,
                () -> aspect.doAround(joinPoint, rule));

        assertEquals(SeckillResultMessages.SECKILL_TOO_BUSY, exception.getMessage());
        verifyBusinessNeverRuns();
    }

    private void whenTryAcquire(
            String userKey,
            String voucherKey,
            SeckillRateLimitResult result) {
        when(rateLimitService.tryAcquire(userKey, 5, 10, voucherKey, 500, 1))
                .thenReturn(result);
    }

    private void verifyBusinessNeverRuns() {
        try {
            verify(joinPoint, never()).proceed();
        } catch (Throwable throwable) {
            throw new AssertionError(throwable);
        }
    }

    private void injectRateLimitService() throws Exception {
        Field field = SeckillRateLimitAspect.class.getDeclaredField("rateLimitService");
        field.setAccessible(true);
        field.set(aspect, rateLimitService);
    }

    private static class TestController {

        @SeckillRateLimit
        public void seckill(Long voucherId) {
        }
    }
}
