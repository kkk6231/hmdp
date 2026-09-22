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
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
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

        checkRateLimit(voucherId, currentUser.getId(), seckillRateLimit);

        return joinPoint.proceed();
    }

    private void checkRateLimit(
            Long voucherId,
            Long userId,
            SeckillRateLimit rule) {
        String userKey = SeckillRedisKeys.rateLimitUserKey(voucherId, userId);
        String voucherKey = SeckillRedisKeys.rateLimitVoucherKey(voucherId);

        SeckillRateLimitResult result = rateLimitService.tryAcquire(
                userKey,
                rule.userMaxCount(),
                rule.userWindowSeconds(),
                voucherKey,
                rule.voucherMaxCount(),
                rule.voucherWindowSeconds());

        if (result == SeckillRateLimitResult.USER_LIMITED) {
            throw new RateLimitException(SeckillResultMessages.USER_REQUEST_TOO_FREQUENT);
        }
        if (result == SeckillRateLimitResult.VOUCHER_LIMITED) {
            throw new RateLimitException(SeckillResultMessages.SECKILL_TOO_BUSY);
        }
        if (result != SeckillRateLimitResult.PASS) {
            throw new IllegalStateException("未知的秒杀限流结果：" + result);
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

}
