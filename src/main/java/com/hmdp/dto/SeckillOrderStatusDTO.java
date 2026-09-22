package com.hmdp.dto;

import com.hmdp.constant.SeckillOrderStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderStatusDTO {

    private Long orderId;
    private Long voucherId;
    private SeckillOrderStatus status;
}
