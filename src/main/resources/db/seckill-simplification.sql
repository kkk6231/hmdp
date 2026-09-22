-- 已有数据库升级脚本：用唯一索引作为“一人一单”的最终防线。
-- 执行前请先确认历史数据中不存在相同 user_id、voucher_id 的重复订单。
ALTER TABLE `tb_voucher_order`
    ADD UNIQUE KEY `uk_user_voucher` (`user_id`, `voucher_id`);
