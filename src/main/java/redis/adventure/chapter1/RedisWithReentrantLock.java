package redis.adventure.chapter1;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.security.Key;
import java.util.HashMap;
import java.util.Map;

/**
 * Redis分布式锁的可重入性设置
 * 可重入锁实际上加重了客户端的复杂性，在编写业务方法时注意在逻辑结构上进行调整则完全不使用重入锁。
 * 下面是Redis分布式可重入锁的简单实现。
 *
 * @author yitian
 */
public class RedisWithReentrantLock {
    private ThreadLocal<Map<String, Integer>> lockers = new ThreadLocal<Map<String, Integer>>();
    private Jedis jedis;

    public RedisWithReentrantLock(Jedis jedis) {
        this.jedis = jedis;
    }

    private boolean _lock(String key) {
        // Jedis版本3.0之前可以使用该方法，3.0之后方法修改了！
        return jedis.set(key, "", "nx", "ex", 5L) != null;
    }

    private void _unlock(String key) {
        jedis.del(key);
    }

    private Map<String, Integer> currentLockers() {
        Map<String, Integer> refs = lockers.get();
        if (refs != null) {
            return refs;
        }
        lockers.set(new HashMap<String, Integer>());
        return lockers.get();
    }

    public boolean lock(String key) {
        Map<String, Integer> refs = currentLockers();
        Integer refCnt = refs.get(key);
        if (refCnt != null) {
            refs.put(key, refCnt + 1);
            return true;
        }

        boolean ok = this._lock(key);
        if (!ok) {
            return false;
        }
        refs.put(key, 1);
        return true;
    }

    public boolean unlock(String key) {
        Map<String, Integer> refs = currentLockers();
        Integer refCnt = refs.get(key);
        if (refCnt == null) {
            return false;
        }

        refCnt -= 1;
        if (refCnt > 0) {
            refs.put(key, refCnt);
        } else {
            refs.remove(key);
            this._unlock(key);
        }
        return true;
    }

    public static void main(String[] args) {
        Jedis jedis = new Jedis("localhost:6379");
        RedisWithReentrantLock redisWithReentrantLock = new RedisWithReentrantLock(jedis);

        System.out.println(redisWithReentrantLock.lock("testkey1"));
        System.out.println(redisWithReentrantLock.lock("testkey1"));
        System.out.println(redisWithReentrantLock.unlock("testkey1"));
        System.out.println(redisWithReentrantLock.unlock("testkey1"));
    }
}
