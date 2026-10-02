package com.hmdp.constant;

public abstract class RegexPatterns {
    /** 手机号正则。 */
    public static final String PHONE_REGEX = "^1([38][0-9]|4[579]|5[0-3,5-9]|6[6]|7[0135678]|9[89])\\d{8}$";
    /** 邮箱正则。 */
    public static final String EMAIL_REGEX = "^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$";
    /** 4～32 位字母、数字或下划线组成的密码。 */
    public static final String PASSWORD_REGEX = "^\\w{4,32}$";
    /** 6 位数字或字母组成的验证码。 */
    public static final String VERIFY_CODE_REGEX = "^[a-zA-Z\\d]{6}$";
}
