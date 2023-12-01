import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import lock.RedisLock;
import queue.DelayNoDuplicatesRedisQueue;
import queue.SimpleRedisQueue;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Main {
    public static void main(String[] args) {
        // docker run -e "IP=0.0.0.0" -p 7000-7005:7000-7005 grokzen/redis-cluster:latest

        try (var redis = RedisClusterClient.create(RedisURI.Builder.redis("localhost").withPort(7000).build())) {
//            var cache = new CascadingCache(
//                    new GuavaBackedCache(),
//                    new RedisBackedCache(redis),
//                    redis
//            );
//            System.out.println("Before put: " + cache.get("abc"));
//            System.out.println("After put: " + cache.get("abc", s -> "123"));
//            cache.remove("abc");
//            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
//            System.out.println("After remove: " + cache.get("abc"));


            //redis.connect().sync().del("/RedisRateLimiter/myKey");
//            var rateLimiter = new RedisRateLimiter(2, redis);
//            System.out.println(rateLimiter.exceedingRateLimit("myKey"));
//            System.out.println(rateLimiter.exceedingRateLimit("myKey"));
//            System.out.println(rateLimiter.exceedingRateLimit("myKey"));
//            System.out.println(rateLimiter.exceedingRateLimit("myKey"));


//            var redisQueue = new SimpleRedisQueue(redis, "myQueue");
//            System.out.println("1st peek: " + redisQueue.peek());
//            System.out.println("Offered: " + redisQueue.offer("first"));
//            System.out.println("2nd peek: " + redisQueue.peek());
//            System.out.println("1st poll: " + redisQueue.poll());
//            System.out.println("2nd poll: " + redisQueue.poll());


//            var redisQueue = new DelayNoDuplicatesRedisQueue(redis, "myQueue2", 1900);
//            System.out.println("1st poll: " + redisQueue.poll());
//            System.out.println("Offered: " + redisQueue.offer("first"));
//            System.out.println("Offered: " + redisQueue.offer("first"));
//            System.out.println("2nd poll: " + redisQueue.poll());
//            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
//            System.out.println("3rd poll: " + redisQueue.poll());
//            System.out.println("4th poll: " + redisQueue.poll());


            redis.connect().sync().del("/RedisLock/foo");
            var random = new Random(System.currentTimeMillis());
            var runnable1 = new Runnable() {
                @Override
                public void run() {
                    var lock = new RedisLock("foo", redis);
                    while (true) {
                        try {
                            lock.lock();
                            System.out.println("Locked 1");
                            LockSupport.parkNanos(MILLISECONDS.toNanos(random.nextInt(1000)));
                        } finally {
                            lock.unlock();
                            System.out.println("Unlocked 1");
                            LockSupport.parkNanos(MILLISECONDS.toNanos(random.nextInt(100)));
                        }
                    }
                }
            };
            var runnable2 = new Runnable() {
                @Override
                public void run() {
                    var lock = new RedisLock("foo", redis);
                    while (true) {
                        try {
                            lock.lock();
                            System.out.println("Locked 2");
                            LockSupport.parkNanos(MILLISECONDS.toNanos(random.nextInt(1000)));
                        } finally {
                            lock.unlock();
                            System.out.println("Unlocked 2");
                            LockSupport.parkNanos(MILLISECONDS.toNanos(random.nextInt(100)));
                        }
                    }
                }
            };
            Thread t1 = new Thread(runnable1);
            Thread t2 = new Thread(runnable2);
            t1.start();
            t2.start();
            LockSupport.parkNanos(SECONDS.toNanos(10));
            System.exit(0);
        }
    }
}