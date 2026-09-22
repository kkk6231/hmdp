package com.hmdp.utils;

import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IpUtilsTest {

    @Test
    void shouldUseFirstIpFromForwardedForHeader() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader("X-Forwarded-For"))
                .thenReturn("192.168.1.10, 10.0.0.1");

        assertEquals("192.168.1.10", IpUtils.getClientIp(request));
    }

    @Test
    void shouldFallbackToRemoteAddress() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        assertEquals("127.0.0.1", IpUtils.getClientIp(request));
    }
}
