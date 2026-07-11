package com.tongji.counter.schema;

/**
 * 位图分片配置与帮助函数。
 * 采用固定分片大小，避免单键因用户ID偏移过大而膨胀。
 */
public final class BitmapShard {
    // （32768位 =>4096字节 4KB）
//    为什么是 32,768？
//    4KB 对齐：32,768 位 = 4KB，正好是一个操作系统内存页的大小，有利于内存和 I/O 效率
//    避免单键膨胀：注释里写了"避免单键因用户ID偏移过大而膨胀"——如果所有用户放在一个巨大的位图里，Redis 中单个 key 的 value 会非常大，操作会变慢。分片后每个 key 最多 4KB，读写都很轻量
//    定位快：分片号 = userId / 32768，位偏移 = userId % 32768，都是 O(1) 的整数运算
    public static final int CHUNK_SIZE = 32_768;

    public static long chunkOf(long userId) {
        // 计算分片号：userId / 32768
        // 例如：userId = 12345，chunkOf(userId) = 3
        // 说明：每个分片包含 32768 个用户，第 3 个分片包含用户 12345
        return userId / CHUNK_SIZE;
    }

    public static long bitOf(long userId) {
        // 计算位偏移：userId % 32768
        // 例如：userId = 12345，bitOf(userId) = 12345
        // 说明：每个分片包含 32768 个用户，第 3 个分片的第 12345 位对应用户 12345
        return userId % CHUNK_SIZE;
    }

    private BitmapShard() {}
}
