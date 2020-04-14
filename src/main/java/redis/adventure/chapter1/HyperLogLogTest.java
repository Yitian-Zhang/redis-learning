package redis.adventure.chapter1;

import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Redis高级数据结构HyperLogLog
 * 提供不是特别精确的统计计数方案，标准误差0.81%
 *
 * @author yitian
 */
public class HyperLogLogTest {

    @Test
    public void pfTest() {
        Jedis jedis = new Jedis("host");

        // 加入1000，100000个数据，查看错误率
        for (int i = 0; i < 1000; i++) {
            jedis.pfadd("pfkey", "user" + i);
            long total = jedis.pfcount("pfkey");

            if (total != i + 1) {
                System.out.printf("%d %d\n", total, i + 1);
                break;
            }
        }
        jedis.close();
    }
}
