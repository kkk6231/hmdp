package com.hmdp.aspect;

import com.hmdp.annotation.SeckillRateLimit;
import com.hmdp.constant.SeckillRedisKeys;
import com.hmdp.constant.SeckillResultMessages;
import com.hmdp.dto.UserDTO;
import com.hmdp.exception.RateLimitException;
import com.hmdp.service.IRateLimitService;
import com.hmdp.utils.IpUtils;
import com.hmdp.utils.UserHolder;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;

/**
 * 秒杀接口限流切面。
 *
 * <p>切面只负责提取当前请求中的业务维度并组织限流规则，具体限流算法由
 * {@link IRateLimitService} 实现。</p>
 */
@Aspect
@Component
public class SeckillRateLimitAspect {

    @Resource
    private IRateLimitService rateLimitService;

    @Around("@annotation(seckillRateLimit)")
    public Object doAround(
            ProceedingJoinPoint joinPoint,
            SeckillRateLimit seckillRateLimit) throws Throwable {
        Long voucherId = getVoucherId(joinPoint);
        UserDTO currentUser = UserHolder.getUser();
        if (currentUser == null || currentUser.getId() == null) {
            throw new IllegalStateException("秒杀限流时未获取到当前登录用户");
        }

        checkUserRateLimit(voucherId, currentUser.getId(), seckillRateLimit);

        String clientIp = IpUtils.getClientIp(getCurrentRequest());
        checkIpRateLimit(voucherId, clientIp, seckillRateLimit);

        return joinPoint.proceed();
    }

    private void checkUserRateLimit(
            Long voucherId,
            Long userId,
            SeckillRateLimit rule) {
        String limitKey = SeckillRedisKeys.rateLimitUserKey(voucherId, userId);
        boolean allowed = rateLimitService.tryAcquire(
                limitKey, rule.userMaxCount(), rule.userWindowSeconds());
        rejectIfNecessary(allowed);
    }

    private void checkIpRateLimit(
            Long voucherId,
            String clientIp,
            SeckillRateLimit rule) {
        String limitKey = SeckillRedisKeys.rateLimitIpKey(voucherId, clientIp);
        boolean allowed = rateLimitService.tryAcquire(
                limitKey, rule.ipMaxCount(), rule.ipWindowSeconds());
        rejectIfNecessary(allowed);
    }

    private void rejectIfNecessary(boolean allowed) {
        if (!allowed) {
            throw new RateLimitException(SeckillResultMessages.REQUEST_TOO_FREQUENT);
        }
    }

    private Long getVoucherId(ProceedingJoinPoint joinPoint) {
        Long voucherId = Arrays.stream(joinPoint.getArgs())
                .filter(Long.class::isInstance)
                .map(Long.class::cast)
                .findFirst()
                .orElse(null);
        if (voucherId == null) {
            throw new IllegalStateException(
                    "@SeckillRateLimit 只能用于包含 Long 类型优惠券 ID 参数的方法");
        }
        return voucherId;
    }

    private HttpServletRequest getCurrentRequest() {
        RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
        if (!(attributes instanceof ServletRequestAttributes)) {
            throw new IllegalStateException("当前上下文不存在 HTTP 请求");
        }
        return ((ServletRequestAttributes) attributes).getRequest();
    }
}
