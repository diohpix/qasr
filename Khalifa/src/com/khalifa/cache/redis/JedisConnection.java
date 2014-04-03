package com.khalifa.cache.redis;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.HierarchicalConfiguration;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.Hashing;

import com.khalifa.util.CommonData;

public class JedisConnection {
// TODO IP CHANGE 107 -> 108
	private static final String JEDIS_PASSWORD = null;//"interest";
	private static JedisPool pool = null;
	private static ShardedJedisPool shardedPool = null;
	private static boolean checkjedis=  false;
	static{
		
	}
	public JedisConnection() {
	}
	
	public static void setCheckJEDIS(){
		checkjedis = true;
		System.out.println("CHECK");
		(new Thread(){
			public void run(){
				while(true){
					try {
						Thread.sleep(5000);
						ShardedJedis jedis = shardedPool.getResource();
						jedis.get("OK".getBytes());
						checkjedis = false;
						System.out.println("SUCCESS!");
						break;
					} catch (Throwable e) {
					}
				}
			}
		}).start();
	}
	private static JedisPoolConfig getPoolConfig(){
		
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(CommonData.redis_maxActive); 
		config.setMaxIdle(CommonData.redis_maxIdle);
		config.setMaxWait(CommonData.redis_maxWait);
		config.setTestOnBorrow(CommonData.redis_testOnBorrow);
		config.setTestWhileIdle(CommonData.redis_testWhileIdle);
		config.minEvictableIdleTimeMillis = CommonData.redis_evictableIdleTimeMillis;
		config.timeBetweenEvictionRunsMillis = CommonData.redis_timeBetweenEvictionRunsMillis;
//		config.numTestsPerEvictionRun = -1;
		return config;
	}
	
	public static JedisPool getInstance(){
		if(pool==null){
			try{
				List<HierarchicalConfiguration> cc = CommonData.redis_servers_host;
				for (HierarchicalConfiguration host : cc) {
					String ip = host.getString("ip");
					String port = host.getString("port");
					pool = new JedisPool(getPoolConfig(),ip.toString(),Integer.parseInt(port), Protocol.DEFAULT_TIMEOUT+1000);
				}
			}catch(Exception e){
				throw new RedisException("Jedis Connection Pool Excption!");
			}
		}
		return pool;
	}
	
	public static ShardedJedisPool getShardedInstance(){
		if(checkjedis ) return null;
		if(shardedPool==null ) {
			List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
			JedisShardInfo jsi = null;
			List<HierarchicalConfiguration> cc = CommonData.redis_servers_host;
			for (HierarchicalConfiguration host : cc) {
				String ip = host.getString("ip");
				String port = host.getString("port");
				jsi = new JedisShardInfo(ip, Integer.valueOf( port), 1000);
				jsi.setPassword(JEDIS_PASSWORD);
			    shards.add( jsi );
			}
		    shardedPool = new ShardedJedisPool(getPoolConfig(), shards, Hashing.MURMUR_HASH, ShardedJedis.DEFAULT_KEY_TAG_PATTERN);
		}
		return shardedPool;
	}
	
	public static void disconnect(Jedis jedis){
		if(jedis.isConnected()) jedis.disconnect();
	}
}