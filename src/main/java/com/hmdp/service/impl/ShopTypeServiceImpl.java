package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryList() {
        // 1.先查redis
        String json = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOPTYPE_KEY);
        // 2.如果存在，直接返回
        if(json != null){
            List<ShopType> shopType = JSONUtil.toList(json, ShopType.class);
            return Result.ok(shopType);
        }
        // 3.如果不存在，去数据库中查询
        List<ShopType> typeList = query().orderByAsc("sort").list();
        // 4.存入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOPTYPE_KEY, JSONUtil.toJsonStr(typeList));
        return Result.ok(typeList);
    }
}
