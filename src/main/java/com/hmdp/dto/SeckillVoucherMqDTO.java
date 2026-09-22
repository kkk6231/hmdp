package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SeckillVoucherMqDTO {

    /**
     * 秒杀订单 ID，由生产者在执行 Lua 脚本前生成。
     */
    private Long orderId;

    /**
     * 下单用户 ID。
     */
    private Long userId;

    /**
     * 秒杀券 ID。
     */
    private Long voucherId;
}
