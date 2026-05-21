package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient2;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 商户服务实现类
 * </p>
 */
@Service
@Slf4j
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 建立线程池，减少线程的创建与销毁
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Resource(name = "cacheClient2")
    private CacheClient2 cacheClient;

    @Override
    public Result queryById(Long id) {

        /*
        String shopKey = CACHE_SHOP_KEY + id;
        // 1.从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (shopJson.isNotBlank()){
            // 3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }
        // 4.不存在，查询数据库
        Shop shop = getById(id)
        // 5.不存在，返回错误
        if (shop == null){
            return Result.fail(“商户不存在”);
        }
        // 6.存在，将数据存入redis缓存
        stringRedisTemplate.opsForValue()
                           .set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 7.返回
        return Result.ok(shop);
         */

        /*
        // 使用缓存空值的方法针对缓存穿透进行修改
        String shopKey = CACHE_SHOP_KEY + id;
        // 1.从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (shopJson.isNotBlank()){
            // 3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }
        // 不存在有两种情况：Redis中命中空值、未命中
        // 4.命中空值，返回错误信息
        if (shopJson != null) {
            return Result.ok("店铺信息不存在");
        }
        // 5.未命中，查询数据库
        Shop shop = getById(id)
        // 6.不存在，先将NULL存入缓存，再返回错误
        if (shop == null){
        stringRedisTemplate.opsForValue()
                           .set(shopKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return Result.fail(“商户不存在”);
        }
        // 7.存在，将数据存入redis缓存
        stringRedisTemplate.opsForValue()
                           .set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 8.返回
        return Result.ok(shop);
         */

        Shop shop = null;
        // 基于redis互斥锁解决缓存击穿
//        shop = queryWithMutex(id);

        // 基于逻辑删除方式解决缓存击穿
        shop = queryWithExpireTime(id);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);

    }

    /**
     * 基于互斥锁解决缓存击穿问题
     * @param id 商户id
     * @return 商户对象或者空值
     */
    private Shop queryWithMutex(Long id){
        String shopKey = CACHE_SHOP_KEY + id;
        // 1.从redis中查询商铺缓存
        String shopCache = stringRedisTemplate.opsForValue().get(shopKey);
        // 2.判断redis中商户是否存在
        if (StrUtil.isNotBlank(shopCache)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(shopCache, Shop.class);
        }
        // 不存在，但命中空值
        if (shopCache != null) {
            return null;
        }
        // 未命中
        // 4.缓存重建
        // 4.1获取互斥锁
        String lockKey = SHOP_LOCK_KEY + id;
        Shop shop = null;
        try{
            boolean isLock = tryLock(lockKey);
            // 4.2判断是否获取成功
            if (!isLock) {
                // 4.3失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 4.4获取锁成功，查询数据库
            shop = getById(id);
            // 模拟重建延迟
            Thread.sleep(500);
            // 5.不存在，返回错误并将空值写入redis
            if (shop == null) {
                stringRedisTemplate.opsForValue()
                        .set(shopKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 6.存在，写入redis
            stringRedisTemplate.opsForValue()
                    .set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }catch (InterruptedException e){
            throw new RuntimeException(e);
        }finally {
            // 7.释放互斥锁
            unLock(lockKey);
        }
        // 8.返回
        return shop;
    }

    /**
     * 使用Redis的setnx命令获取一个互斥锁
     * @param lockKey 锁在Redis中的key
     * @return 返回是否成功获得锁
     */
    private boolean tryLock(String lockKey) {
        return BooleanUtil.isTrue(stringRedisTemplate.opsForValue()
                .setIfAbsent(lockKey, "1", 10, TimeUnit.SECONDS));
    }

    /**
     * 释放锁
      */
    private boolean unLock(String lockKey) {
        return BooleanUtil.isTrue(stringRedisTemplate.delete(lockKey));
    }

    /**
     * 基于逻辑过期解决缓存击穿问题
     * @param id 商户id
     * @return 商户对象
     */
    private Shop queryWithExpireTime(Long id) {
        String shopKey = CACHE_SHOP_KEY + id;
        String lockKey = SHOP_LOCK_KEY + id;
        // 1.从redis中查询商铺缓存
        String shopCache = stringRedisTemplate.opsForValue().get(shopKey);
        // 2.判断redis中商户是否存在
        if (StrUtil.isBlank(shopCache)) {
            // 3.由于没有过期时间，因此一定是未命中，不可能为命中空值
            return null;
        }
        // 4.存在，需要把Json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopCache, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 未过期，直接返回店铺信息
            return shop;
        }
        // 已过期，需要缓存重建
        // 6.重建缓存
        // 获取互斥锁
        boolean isLock = tryLock(lockKey);
        // 判断锁是否获取成功
        if (isLock) {

            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try{
                    // 成功，开启独立线程，重建缓存
                    this.saveShopToRedis(id, CACHE_SHOP_TTL);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    // 释放锁
                    unLock(lockKey);
                }
            });
        }
        // 不论是否成功获得锁，都需要携带旧数据继续运行
        return shop;

    }

    /**
     * 提前将数据缓存到redis 设置逻辑过期时间
     * @param id 商户id
     * @param expireMinutes 逻辑过期时间
     */
    public void saveShopToRedis(Long id, long expireMinutes) throws InterruptedException {
        // 在数据库中查询数据
        Shop shop = getById(id);
        // 模拟重建缓存延迟
        Thread.sleep(200);
        // 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusMinutes(expireMinutes));
        // 写入redis
        stringRedisTemplate.opsForValue()
                .set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData), expireMinutes);
    }

    /**
     * 数据库更新，并删除对应的缓存
     * @param shop 商户对象
     * @return 是否成功
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}