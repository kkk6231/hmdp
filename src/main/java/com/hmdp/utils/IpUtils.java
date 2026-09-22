package com.hmdp.utils;

import javax.servlet.http.HttpServletRequest;

/**
 * HTTP 客户端 IP 获取工具。
 */
public final class IpUtils {

    private static final String UNKNOWN = "unknown";

    private IpUtils() {
    }

    public static String getClientIp(HttpServletRequest request) {
        String ip = firstValidHeader(
                request,
                "X-Forwarded-For",
                "X-Real-IP",
                "Proxy-Client-IP",
                "WL-Proxy-Client-IP");

        if (isBlank(ip)) {
            ip = request.getRemoteAddr();
        }
        if (!isBlank(ip) && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }
        return isBlank(ip) ? UNKNOWN : ip;
    }

    private static String firstValidHeader(HttpServletRequest request, String... headerNames) {
        for (String headerName : headerNames) {
            String value = request.getHeader(headerName);
            if (!isBlank(value) && !UNKNOWN.equalsIgnoreCase(value)) {
                return value;
            }
        }
        return null;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
