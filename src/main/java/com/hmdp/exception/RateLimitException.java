package com.hmdp.exception;

/**
 * 请求超过限流阈值时抛出的业务异常。
 */
public class RateLimitException extends RuntimeException {

    public RateLimitException(String message) {
        super(message);
    }
}
