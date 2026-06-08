package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 服务实现类
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl
        extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private VoucherMapper voucherMapper;
    @Resource
    private SeckillVoucherMapper seckillVoucherMapper;

    private Integer stock = null;
    private volatile boolean running = true;

    // 线程代理对象，用于开启事务
    IVoucherOrderService proxy;

    // 秒杀脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 单线程的线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    /**
     * 阻塞队列消费订单数据
     * 当线程从空的阻塞队列获取数据时会发生阻塞
     */
    private static BlockingQueue<VoucherOrder> orderTasks =
            new ArrayBlockingQueue<>(1000 * 24);


    @PostConstruct
    private void init() {
        initStreamGroup();
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    @PreDestroy
    private void destroy() {
        running = false;
        SECKILL_ORDER_EXECUTOR.shutdownNow();
    }

    private void initStreamGroup() {
        try {
            stringRedisTemplate.execute((RedisCallback<Object>) connection -> {
                connection.execute("XGROUP", "CREATE".getBytes(), "stream.orders".getBytes(),
                        "g1".getBytes(), "0".getBytes(), "MKSTREAM".getBytes());
                return null;
            });
        } catch (Exception e) {
            String message = e.getMessage();
            if (message == null || !message.contains("BUSYGROUP")) {
                log.warn("Init Redis stream group failed", e);
            }
        }
    }

    /**
     * 基于Redis-Stream消息队列执行异步下单
     * 操作数据库
     */
    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (running) {
                try{
                    // 获取消息队列中的订单信息 XREADGROUP GROUP g1 COUNT 1 BLOCK 2000 STREAMS xxx >
                    List<MapRecord<String, Object, Object>> list =  stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 判断是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 如果失败。说明没有消息，继续下一次循环
                        continue;
                    }
                    // 解析消息队列中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values =  record.getValue();
                    VoucherOrder voucherOrder =  BeanUtil.fillBeanWithMap(
                            values, new VoucherOrder(), true);
                    // 如果成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                }catch(Exception e){
                    if (!running) {
                        break;
                    }
                    log.error("订单处理异常！", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (running) {
                try{
                    // 获取PendingList中的订单信息 XREADGROUP GROUP g1 COUNT 1 BLOCK 2000 STREAMS xxx >
                    List<MapRecord<String, Object, Object>> list =  stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 判断是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 如果失败。说明PendingList没有消息，结束循环
                        break;
                    }
                    // 解析消息队列中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values =  record.getValue();
                    VoucherOrder voucherOrder =  BeanUtil.fillBeanWithMap(
                            values, new VoucherOrder(), true);
                    // 如果成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                }catch(Exception e){
                    if (!running) {
                        break;
                    }
                    log.error("处理PendingList订单抛出异常！", e);
                    try{
                        Thread.sleep(2000);
                    }catch(InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                }
            }
        }
    }



    /**
     * 基于Redis-Stream消息队列——创建订单
     * @param voucherOrder
     */
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 获取用户
        Long userId = voucherOrder.getUserId();
        // 创建锁对象
        RLock lock = redissonClient.getLock("lock_order_" + userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        // 判断锁是否获取成功
        if (!isLock) {
            // 获取锁失败，返回错误或重试
            log.error("不允许重复下单");
            return;
        }
        try{
            proxy.createVoucherOrder(voucherOrder);
        }finally {
            lock.unlock();
        }
    }



    /**
     * 基于Redis-Stream消息队列创建订单
     * @param voucherOrder
     */
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("不允许重复下单！");
                return;
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                log.error("库存不足！");
                return;
            }

            // 7.创建订单
            save(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    /**
     * 基于消息队列完成秒杀业务
     * @param voucherId
     * @return
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 获取订单id
        Long orderId = redisIdWorker.nextId("order");
        // 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        // 判断结果是否为0
        int r = result.intValue();
        if (r != 0){
            // 不是0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 返回
        return Result.ok(orderId);
    }

    //    /**
//     * 基于阻塞队列执行异步下单
//     * 操作数据库
//     */
//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while (true) {
//                try{
//                    // 获取阻塞队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 创建订单
//                    handleVoucherOrder(voucherOrder);
//                }catch(Exception e){
//                    log.error("订单处理异常！", e);
//                }
//            }
//        }
//    }

//    /**
//     * 基于阻塞队列——创建订单
//     * @param voucherOrder
//     */
//    private void handleVoucherOrder(VoucherOrder voucherOrder) {
//        // 获取用户
//        Long userId = voucherOrder.getUserId();
//        // 创建锁对象
//        RLock lock = redissonClient.getLock("lock_order_" + userId);
//        // 获取锁
//        boolean isLock = lock.tryLock();
//        // 判断锁是否获取成功
//        if (!isLock) {
//            // 获取锁失败，返回错误或重试
//            log.error("不允许重复下单");
//            return;
//        }
//        try{
//            proxy.createVoucherOrder(voucherOrder);
//        }finally {
//            lock.unlock();
//        }
//    }

//    /**
//     * 基于阻塞队列实现的新的创建订单
//     * @param voucherOrder
//     */
//    public void createVoucherOrder(VoucherOrder voucherOrder) {
//        Long userId = voucherOrder.getUserId();
//        Long voucherId = voucherOrder.getVoucherId();
//        // 创建锁对象
//        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
//        // 尝试获取锁
//        boolean isLock = redisLock.tryLock();
//        // 判断
//        if (!isLock) {
//            // 获取锁失败，直接返回失败或者重试
//            log.error("不允许重复下单！");
//            return;
//        }
//
//        try {
//            // 5.1.查询订单
//            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//            // 5.2.判断是否存在
//            if (count > 0) {
//                // 用户已经购买过了
//                log.error("不允许重复下单！");
//                return;
//            }
//
//            // 6.扣减库存
//            boolean success = seckillVoucherService.update()
//                    .setSql("stock = stock - 1") // set stock = stock - 1
//                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
//                    .update();
//            if (!success) {
//                // 扣减失败
//                log.error("库存不足！");
//                return;
//            }
//
//            // 7.创建订单
//            save(voucherOrder);
//        } finally {
//            // 释放锁
//            redisLock.unlock();
//        }
//    }

//    /**
//     * 接受下单请求，检测是否允许下单，并将订单信息提交至阻塞队列
//     * @param voucherId
//     * @return
//     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 获取用户
//        Long userId = UserHolder.getUser().getId();
//        // 执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        // 判断结果是否为0
//        int r = result.intValue();
//        if (r != 0){
//            // 不是0，代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//        // 是0，有购买资格，将下单信息添加到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        Long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//
//        // 放入阻塞队列
//        orderTasks.add(voucherOrder);
//
//        // 获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        // 返回
//        return Result.ok(orderId);
//    }

//    /**、
//     * 优惠券秒杀
//     * 由于本方法内仅有查询数据库操作，因此不需要添加事务
//     * @param voucherId 优惠券ID
//     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Voucher voucher = voucherMapper.selectById(voucherId);
//        if (voucher == null) {
//            return Result.fail("优惠券信息不存在");
//        }
//        // 优惠券使用时间信息
//        SeckillVoucher seckillVoucher = seckillVoucherMapper.selectById(voucherId);
//        LocalDateTime beginTime = seckillVoucher.getBeginTime();
//        LocalDateTime endTime = seckillVoucher.getEndTime();
//
//        if (beginTime.isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀活动尚未开始！");
//        }
//        if (endTime.isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀活动已结束！");
//        }
//        // 优惠券剩余库存数量
//        Integer stock = seckillVoucher.getStock();
//        if (stock <= 0) {
//            return Result.fail("优惠券秒杀完毕库存不足！！！");
//        }
//
//        // 调用一人一单方法
//        // 仅对值相同的userId加锁
//        Long userId = UserHolder.getUser().getId();
//        // 其一：使用悲观锁
////        synchronized (userId.toString().intern()) {
////            // 获取代理对象，否则事务会失效
////            IVoucherOrderService iVoucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
////            return iVoucherOrderService.createVoucherOrder(voucherId);
////        }
//        // 其二：使用分布式锁
////        // 创建锁对象
////        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
////        // 获取锁
////        boolean isLock = lock.tryLock(5);
////        // 判断是否获取成功
////        if (!isLock) {
////            // 获取锁失败，返回错误
////            return Result.fail("请勿重复下单");
////        }
////        try{
////            IVoucherOrderService iVoucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
////            return iVoucherOrderService.createVoucherOrder(voucherId);
////        }finally {
////            lock.unlock();
////        }
//        // 其三：使用Redisson分布式锁
//        // 创建对象
//        RLock lock = redissonClient.getLock("order:" + userId);
//        // 获取锁
//        // 设置参数：最大等待时长，实现可重试锁
//        boolean tryLock = lock.tryLock();
//        if (!tryLock) {
//            // 如果未获取到锁，直接返回用户不可重复下单
//            return Result.fail("用户不可重复下单购买，一人只能买一次");
//        }
//        // 获取代理对象
//        IVoucherOrderService iVoucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
//        try {
//            return iVoucherOrderService.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//
//        // 调用CAS方法解决超卖问题
////        return seckillVoucherWithStockMysql(voucherId);
//    }

    /**
     * 基于乐观锁（CAS法）解决超卖问题
     * @param voucherId
     * @return
     */
    @Override
    @Transactional
    public Result seckillVoucherWithStockMysql(Long voucherId) {
        // 更新时检查此时库存与此前检查库存数量是否一致：CAS   ————失败率太高
        // 对于库存，实际上只要库存仍大于0就可以并发修改，不会引起业务问题
        boolean success = seckillVoucherService.update()
                .setSql(" stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0)
                .update();
        if (!success) {
            return Result.fail("优惠券秒杀完毕库存不足！！！");
        }

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(redisIdWorker.nextId("order"));
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUpdateTime(LocalDateTime.now());
        save(voucherOrder);
        return Result.ok("抢购成功");
    }

    @Override
    public Result seckillVoucherByUser(Long voucherId) {
        Voucher voucher = voucherMapper.selectById(voucherId);
        if (voucher == null) {
            return Result.fail("优惠券信息不存在");
        }
        // 优惠券使用时间信息
        SeckillVoucher seckillVoucher = seckillVoucherMapper.selectById(voucherId);
        LocalDateTime beginTime = seckillVoucher.getBeginTime();
        LocalDateTime endTime = seckillVoucher.getEndTime();

        if (beginTime.isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀活动尚未开始！");
        }
        if (endTime.isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀活动已结束！");
        }
        // 优惠券剩余库存数量
        Integer stock = seckillVoucher.getStock();
        if (stock <= 0) {
            return Result.fail("优惠券秒杀完毕库存不足！！！");
        }
        // 获取代理对象
        IVoucherOrderService iVoucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
        // 根据userId加锁，不同用户不会被锁定，userId.toString()方法中会每次都会产生不同的userId，所以起不到锁定作用，intern()会从jvm中的常量池中去匹配userId
        // 不将synchronized加在createVoucherOrder()方法上是因为锁粒度变大，锁的对象为this，多线程执行方法为串行执行，效率低，对userId加锁是相当对每一个用户进行加锁处理，锁粒度变小
        Long userId = UserHolder.getUser().getId();
        synchronized (userId.toString().intern()) {
            return iVoucherOrderService.createVoucherOrder(voucherId);
        }
    }

    @Override
    public Result seckillVoucherWithRedis(Long voucherId) {
        Voucher voucher = voucherMapper.selectById(voucherId);
        if (voucher == null) {
            return Result.fail("优惠券信息不存在");
        }
        // 优惠券使用时间信息
        SeckillVoucher seckillVoucher = seckillVoucherMapper.selectById(voucherId);
        LocalDateTime beginTime = seckillVoucher.getBeginTime();
        LocalDateTime endTime = seckillVoucher.getEndTime();

        if (beginTime.isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀活动尚未开始！");
        }
        if (endTime.isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀活动已结束！");
        }
        // 优惠券剩余库存数量
        Integer stock = seckillVoucher.getStock();
        if (stock <= 0) {
            return Result.fail("优惠券秒杀完毕库存不足！！！");
        }
        Long userId = UserHolder.getUser().getId();
        // 获取redis锁对象
        SimpleRedisLock redisLock2 = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        boolean tryLock = redisLock2.tryLock(100);
        // 如果未获取到锁，直接返回用户不可重复下单
        if (!tryLock) {
            return Result.fail("用户不可重复下单购买，一人只能买一次");
        }
        // 获取代理对象
        IVoucherOrderService iVoucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
        try {
            return iVoucherOrderService.createVoucherOrder(voucherId);
        } finally {
            redisLock2.unlock();
        }
    }

    /**
     * 封装新的需求：一人一单
     * 创建订单
     * 由于多线程并发时需要检测该ID是否存在，因此使用悲观锁
     * @param voucherId
     * @return
     */
    @Transactional
    @Override
    public Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            return Result.fail("该用户已抢购过该优惠券");
        }
        // 更新时判断是否库存是否大于0 乐观锁
        boolean success = seckillVoucherService.update()
                .setSql(" stock = stock - 1")
                .gt("stock", 0).eq("voucher_id", voucherId)
                .update();
        if (!success) {
            return Result.fail("优惠券秒杀完毕库存不足！！！");
        }

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(userId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUpdateTime(LocalDateTime.now());
        save(voucherOrder);
        return Result.ok("抢购成功");
    }

    @Override
    public Result seckillVoucherWithRedissonOptimization(Long voucherId) {

        long orderId = redisIdWorker.nextId("order");
        Long userId = UserHolder.getUser().getId();
        Long seckillFlag = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        if (seckillFlag != 0) {
            return Result.fail(seckillFlag == 1 ? "库存不足" : "同一用户不可重复抢购");
        }
        return Result.ok(orderId);
    }
}
