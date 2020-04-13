package redis.adventure.chapter1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Type;
import java.util.Set;
import java.util.UUID;

/**
 * 使用Redis实现延迟队列
 * @param <T>
 *
 * @author yitian
 */
public class RedisDelayingQueue<T> {

    static class TaskItem<T> {
        public String id;
        public T msg;
    }

    // fastjson序列化对象中存在generic类型时，需要使用TypeReference
    private Type taskType = new TypeReference<TaskItem>(){}.getType();
    private Jedis jedis;
    private String queueKey;

    public RedisDelayingQueue(Jedis jedis, String queueKey) {
        this.jedis = jedis;
        this.queueKey = queueKey;
    }

    /**
     * 向队列中写入数据
     * @param msg 待写入的消息，会被加上ID封装为TaskItem
     */
    public void delay(T msg) {
        // 创建Task并分配位移的ID
        TaskItem<T> task = new TaskItem<T>();
        task.id = UUID.randomUUID().toString();
        task.msg = msg;

        // 使用fastjson进行序列化
        String s = JSON.toJSONString(task);
        // 将数据加入到延迟队列中，5s后再试
        jedis.zadd(queueKey, System.currentTimeMillis() + 5000, s);
    }

    /**
     * 消息出队列方法
     */
    public void loop() {
        while (!Thread.interrupted()) {
            // 获取zset集合中的一条记录
            Set<String> values = jedis.zrangeByScore(queueKey, 0, System.currentTimeMillis(), 0, 1);
            // 如果当前延时队列为空则sleep后再试
            if (values.isEmpty()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
                continue;
            }
            // 获取数据后进行处理
            String s = values.iterator().next();
            if (jedis.zrem(queueKey, s) > 0) { // 使用redis的zrem方法在多并发环境中抢任务，确定消息的所属对象
                TaskItem<T> task = JSON.parseObject(s, taskType); // 反序列化消息后进行使用
                this.handleMsg(task.msg);
            }

        }
    }

    public void handleMsg(T msg) {
        System.out.println(msg);
    }

    /**
     * 测试方法
     */
    public static void main(String[] args) {
        Jedis jedis = new Jedis("host");
        final RedisDelayingQueue<String> queue = new RedisDelayingQueue<String>(jedis, "q-demo");

        // 创建producer线程，并使用delay方法向DelayQueue中写入数据
        Thread producer = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    queue.delay("data" + i);
                }
            }
        };

        // 创建consumer线程，并使用loop方法从delayQueue中获取数据并消费
        Thread consumer = new Thread() {
            @Override
            public void run() {
                queue.loop();
            }
        };

        // 分别启动两个线程
        producer.start();
        consumer.start();

        try {
            // 阻塞一个线程
            producer.join();
            Thread.sleep(6000);
            consumer.interrupt();
            consumer.join();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
