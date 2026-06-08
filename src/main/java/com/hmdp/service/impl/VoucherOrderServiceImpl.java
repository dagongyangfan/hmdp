package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

@Slf4j
@Service
public class VoucherOrderServiceImpl
        extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    private static final String STREAM_ORDERS = "stream.orders";
    private static final String GROUP_NAME = "g1";
    private static final String CONSUMER_NAME = "c1";
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ApplicationContext applicationContext;

    private volatile boolean running = true;
    private IVoucherOrderService proxy;

    @PostConstruct
    private void init() {
        initStreamGroup();
        proxy = applicationContext.getBean(IVoucherOrderService.class);
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    @PreDestroy
    private void destroy() {
        running = false;
        SECKILL_ORDER_EXECUTOR.shutdownNow();
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        return seckillVoucherWithRedissonOptimization(voucherId);
    }

    @Override
    public Result seckillVoucherWithRedissonOptimization(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        if (seckillVoucher == null) {
            return Result.fail("优惠券不存在");
        }

        LocalDateTime now = LocalDateTime.now();
        if (seckillVoucher.getBeginTime().isAfter(now)) {
            return Result.fail("秒杀活动尚未开始");
        }
        if (seckillVoucher.getEndTime().isBefore(now)) {
            return Result.fail("秒杀活动已经结束");
        }

        initRedisStockIfAbsent(seckillVoucher);
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        int code = result == null ? 1 : result.intValue();
        if (code != 0) {
            return Result.fail(code == 1 ? "库存不足" : "同一用户不可重复抢购");
        }
        return Result.ok(orderId);
    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            log.warn("Duplicate voucher order ignored, userId={}, voucherId={}", userId, voucherId);
            return;
        }

        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!success) {
            log.warn("Voucher stock is insufficient while creating order, userId={}, voucherId={}", userId, voucherId);
            return;
        }

        voucherOrder.setPayType(1);
        voucherOrder.setStatus(1);
        save(voucherOrder);
    }

    @Override
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(redisIdWorker.nextId("order"));
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);
        createVoucherOrder(voucherOrder);
        return Result.ok(voucherOrder.getId());
    }

    @Override
    public Result seckillVoucherWithStockMysql(Long voucherId) {
        return createVoucherOrder(voucherId);
    }

    @Override
    public Result seckillVoucherByUser(Long voucherId) {
        return createVoucherOrder(voucherId);
    }

    @Override
    public Result seckillVoucherWithRedis(Long voucherId) {
        return seckillVoucherWithRedissonOptimization(voucherId);
    }

    private void initRedisStockIfAbsent(SeckillVoucher seckillVoucher) {
        String key = SECKILL_STOCK_KEY + seckillVoucher.getVoucherId();
        long ttlSeconds = Math.max(1, Duration.between(LocalDateTime.now(), seckillVoucher.getEndTime()).getSeconds());
        Boolean absent = stringRedisTemplate.opsForValue().setIfAbsent(
                key,
                String.valueOf(seckillVoucher.getStock()),
                ttlSeconds,
                TimeUnit.SECONDS
        );
        if (Boolean.TRUE.equals(absent)) {
            log.info("Initialized seckill stock cache, voucherId={}, stock={}",
                    seckillVoucher.getVoucherId(), seckillVoucher.getStock());
        }
    }

    private void initStreamGroup() {
        try {
            stringRedisTemplate.execute((RedisCallback<Object>) connection -> {
                connection.execute("XGROUP", "CREATE".getBytes(), STREAM_ORDERS.getBytes(),
                        GROUP_NAME.getBytes(), "0".getBytes(), "MKSTREAM".getBytes());
                return null;
            });
        } catch (Exception e) {
            String message = e.getMessage();
            if (message == null || !message.contains("BUSYGROUP")) {
                log.warn("Init Redis stream group failed", e);
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.warn("Duplicate voucher order is being processed, userId={}", userId);
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            handlePendingList();
            while (running) {
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(STREAM_ORDERS, ReadOffset.lastConsumed())
                    );
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    handleRecord(list.get(0));
                } catch (Exception e) {
                    if (!running) {
                        break;
                    }
                    log.error("Order handler failed, will try pending list", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (running) {
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(STREAM_ORDERS, ReadOffset.from("0"))
                    );
                    if (list == null || list.isEmpty()) {
                        break;
                    }
                    handleRecord(list.get(0));
                } catch (Exception e) {
                    if (!running) {
                        break;
                    }
                    log.error("Pending order handler failed", e);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        private void handleRecord(MapRecord<String, Object, Object> record) {
            Map<Object, Object> values = record.getValue();
            VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
            handleVoucherOrder(voucherOrder);
            stringRedisTemplate.opsForStream().acknowledge(STREAM_ORDERS, GROUP_NAME, record.getId());
        }
    }
}
