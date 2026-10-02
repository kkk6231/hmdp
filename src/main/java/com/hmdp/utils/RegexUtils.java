package com.hmdp.utils;

import com.hmdp.constant.RegexPatterns;
import cn.hutool.core.util.StrUtil;

public class RegexUtils {
    /** 手机号为空或格式错误时返回 true。 */
    public static boolean isPhoneInvalid(String phone){
        return mismatch(phone, RegexPatterns.PHONE_REGEX);
    }

    /** 邮箱为空或格式错误时返回 true。 */
    public static boolean isEmailInvalid(String email){
        return mismatch(email, RegexPatterns.EMAIL_REGEX);
    }

    /** 验证码为空或格式错误时返回 true。 */
    public static boolean isCodeInvalid(String code){
        return mismatch(code, RegexPatterns.VERIFY_CODE_REGEX);
    }

    private static boolean mismatch(String str, String regex){
        if (StrUtil.isBlank(str)) {
            return true;
        }
        return !str.matches(regex);
    }
}
