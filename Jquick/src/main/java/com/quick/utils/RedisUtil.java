package com.quick.utils;

import org.apache.log4j.Logger;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.util.List;
import java.util.Map;


public class RedisUtil {

    private static Logger logger = Logger.getLogger(RedisUtil.class);

    private static ShardedJedisPool shardedJedisPool;

    public RedisUtil(ShardedJedisPool shardedJedisPool) {
        this.shardedJedisPool = shardedJedisPool;
    }

    /**
     * 删除对应的value
     *
     * @param key
     * @return
     */
    public static Long del(final String key) throws Exception{
        Long result = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            if (exists(key)) {
                result = shardedJedis.del(key);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
        return result;
    }

    /**
     * 删除对应的value
     *
     * @param key
     * @return
     */
    public static Long hdel(final String key, final String... fields) throws Exception{
        Long result = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            result = shardedJedis.hdel(key, fields);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
        return result;
    }

    /**
     * 判断缓存中是否有对应的value
     *
     * @param key
     * @return
     */
    public static boolean exists(String key) throws Exception{
        boolean result = false;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                throw new Exception();
            }
            result = shardedJedis.exists(key);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
        	shardedJedis.close();
//            	shardedJedis.close();
        }
        return result;
    }

    /**
     * 判断缓存中是否有对应的value
     *
     * @param key
     * @return
     */
    public static boolean hexists(String key, String field) throws Exception{
        boolean result = false;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                throw new Exception();
            }
            result = shardedJedis.hexists(key, field);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
        return result;
    }

    /**
     * 获得对象的类型
     * @param key
     * @return
     */
    public static String type(String key) throws Exception{
        String result = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            result = shardedJedis.type(key);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
        return result;
    }

    /**
     * 读取缓存
     *
     * @param key
     * @return
     */
    public static <T> T get(String key, Class<T> clazz) throws Exception{
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            byte[] result = shardedJedis.get(key.getBytes());
            if (result != null) {
                return ProtostuffSerializationUtil.deserialize(result, clazz);
            }
            return null;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
    }

    /**
     * 读取缓存
     *
     * @param key
     * @return
     */

    public static byte[] get(String key) throws Exception{
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            return shardedJedis.get(key.getBytes());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
    }
    public static byte[] getSet(String key, Object value) throws Exception{
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            return shardedJedis.getSet(key.getBytes(), ProtostuffSerializationUtil.serialize(value));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
    }

    /**
     * 读取哈希缓存
     *
     * @param key
     * @return
     */
    public static byte[] hget(String hkey, String key) throws Exception{
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            return shardedJedis.hget(hkey.getBytes(), key.getBytes());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
    }

    /**
     * 写入缓存
     *
     * @param key
     * @param value
     * @return
     */
    public static String set(String key, Object value) throws Exception{
        String result = null;

        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            if (value.getClass().isAssignableFrom(List.class)) {
                if (((List)value).size() == 0) {
                    String message = "序列化的列表长度为0";
                    logger.error(message);
                    throw new Exception(message);
                }
                result = shardedJedis.set(key.getBytes(), ProtostuffSerializationUtil.serializeList((List)value, ((List) value).get(0).getClass()));
            } else {
                result = shardedJedis.set(key.getBytes(), ProtostuffSerializationUtil.serialize(value));
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
        return result;
    }

    /**
     * 写入缓存
     *
     * @param key
     * @param value
     * @param expireTime
     * @return
     */
    public static String set(String key, Object value, int expireTime) throws Exception {
        String result = null;

        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            if (value.getClass().isAssignableFrom(List.class)) {
                if (((List)value).size() == 0) {
                    String message = "序列化的列表长度为0";
                    logger.error(message);
                    throw new Exception(message);
                }
                result = shardedJedis.setex(key.getBytes(), expireTime, ProtostuffSerializationUtil.serializeList((List)value, ((List) value).get(0).getClass()));
            } else {
                result = shardedJedis.setex(key.getBytes(), expireTime, ProtostuffSerializationUtil.serialize(value));
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
        return result;
    }

    public static Long setnx(String key, Object value) throws Exception{
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return 0L;
            }
            if (value.getClass().isAssignableFrom(List.class)) {
                if (((List)value).size() == 0) {
                    String message = "序列化的列表长度为0";
                    logger.error(message);
                    throw new Exception(message);
                }
                return shardedJedis.setnx(key.getBytes(), ProtostuffSerializationUtil.serializeList((List)value, ((List)value).get(0).getClass()));
            } else {
                return shardedJedis.setnx(key.getBytes(), ProtostuffSerializationUtil.serialize(value));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
    }

    public static Long setnx(String key, Object value, int expire) throws Exception{
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return 0L;
            }
            if (value.getClass().isAssignableFrom(List.class)) {
                if (((List)value).size() == 0) {
                    String message = "序列化的列表长度为0";
                    logger.error(message);
                    throw new Exception(message);
                }
                Long result = shardedJedis.setnx(key.getBytes(), ProtostuffSerializationUtil.serializeList((List)value, ((List)value).get(0).getClass()));
                expire(key, expire);
                return result;
            } else {
                Long result = shardedJedis.setnx(key.getBytes(), ProtostuffSerializationUtil.serialize(value));
                expire(key, expire);
                return result;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
    }

    /**
     * 写入哈希缓存
     *
     * @param key
     * @param value
     * @return
     */
    public static Long hset(String hkey, String key, byte[] value) throws Exception{
        Long result = null;

        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            return shardedJedis.hset(hkey.getBytes(), key.getBytes(), value);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
    }


    public static Map<byte[], byte[]> hgetAll(String key) throws Exception{
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            Map<byte[], byte[]> kvMap = shardedJedis.hgetAll(key.getBytes());
            return kvMap;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
    }



    /**
     * 在某段时间后失效
     *
     * @param key
     * @param seconds
     * @return
     */
    public static Long expire(String key, int seconds) throws Exception {
        Long result = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            result = shardedJedis.expire(key.getBytes(), seconds);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
        return result;
    }

    /**
     * 在某个时间点失效
     *
     * @param key
     * @param unixTime
     * @return
     */
    public static Long expireAt(String key, long unixTime) throws Exception {
        Long result = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            result = shardedJedis.expireAt(key, unixTime);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
        return result;
    }

    /**
     * 获得有效期
     * @param key
     * @return
     */
    public static Long ttl(String key) throws Exception {
        Long result = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            if (shardedJedis == null) {
                return null;
            }
            result = shardedJedis.ttl(key);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            	shardedJedis.close();
        }
        return result;
    }

//    private static void returnResource(ShardedJedis shardedJedis) {
//    	
//        if (shardedJedis != null) {
////        	shardedJedisPool.returnResourceObject();
//            shardedJedisPool.	shardedJedis.close();
//        }
//    }
}
