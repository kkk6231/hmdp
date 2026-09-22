package com.hmdp.aspect;

import com.hmdp.annotation.SeckillRateLimit;
import com.hmdp.constant.SeckillRedisKeys;
import com.hmdp.dto.UserDTO;
import com.hmdp.exception.RateLimitException;
import com.hmdp.service.IRateLimitService;
import com.hmdp.utils.UserHolder;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeckillRateLimitAspectTest {

    private final IRateLimitService rateLimitService = mock(IRateLimitService.class);
    private final ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
    private final HttpServletRequest request = mock(HttpServletRequest.class);
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
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));

        when(joinPoint.getArgs()).thenReturn(new Object[]{10L});
        when(request.getHeader("X-Forwarded-For")).thenReturn(null);
        when(request.getHeader("X-Real-IP")).thenReturn(null);
        when(request.getHeader("Proxy-Client-IP")).thenReturn(null);
        when(request.getHeader("WL-Proxy-Client-IP")).thenReturn(null);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    void shouldProceedWhenUserAndIpAreAllowed() throws Throwable {
        String userKey = SeckillRedisKeys.rateLimitUserKey(10L, 7L);
        String ipKey = SeckillRedisKeys.rateLimitIpKey(10L, "127.0.0.1");
        Object expected = new Object();

        when(rateLimitService.tryAcquire(userKey, 5, 10)).thenReturn(true);
        when(rateLimitService.tryAcquire(ipKey, 50, 10)).thenReturn(true);
        when(joinPoint.proceed()).thenReturn(expected);

        Object actual = aspect.doAround(joinPoint, rule);

        assertSame(expected, actual);
        verify(joinPoint).proceed();
    }

    @Test
    void shouldRejectBeforeBusinessMethodWhenUserLimitIsExceeded() {
        String userKey = SeckillRedisKeys.rateLimitUserKey(10L, 7L);
        String ipKey = SeckillRedisKeys.rateLimitIpKey(10L, "127.0.0.1");
        when(rateLimitService.tryAcquire(userKey, 5, 10)).thenReturn(false);

        assertThrows(RateLimitException.class, () -> aspect.doAround(joinPoint, rule));

        verify(rateLimitService, never()).tryAcquire(ipKey, 50, 10);
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
