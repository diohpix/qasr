package me.interest.pi.relay.cache.redis;


import java.util.ArrayList;
import java.util.List;

import me.interest.pi.relay.util.Configure;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.Hashing;

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
					System.out.println("------");
					try {
						Thread.sleep(5000);
						ShardedJedis jedis = shardedPool.getResource();
						jedis.get("OK".getBytes());
						checkjedis = false;
						System.out.println("SUCCESS!");
						break;
					} catch (Throwable e) {
						e.printStackTrace();
						System.out.println("FAIL ");
					}
				}
			}
		}).start();
	}
	private static JedisPoolConfig getPoolConfig(){
		
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(Configure.getIntProperty("redis.maxActive")); 
		config.setMaxIdle(Configure.getIntProperty("redis.maxIdle"));
		config.setMaxWait(Configure.getIntProperty("redis.maxWait"));
		config.setTestOnBorrow(Configure.getBoolProperty("redis.testOnBorrow"));
		config.setTestWhileIdle(Configure.getBoolProperty("redis.testWhileIdle"));
		config.minEvictableIdleTimeMillis = Configure.getIntProperty("redis.evictableIdleTimeMillis");
		config.timeBetweenEvictionRunsMillis = Configure.getIntProperty("redis.timeBetweenEvictionRunsMillis");
//		config.numTestsPerEvictionRun = -1;
		return config;
	}
	
	public static JedisPool getInstance(){
		if(pool==null){
			try{
				List<Object> ss = Configure.getList("redis.servers.host.ip");
		        int redislen = ss.size();
				for(int i=0;i<redislen;i++){
					String ip = Configure.getProperty("redis.servers.host("+i+").ip");
					String port = Configure.getProperty("redis.servers.host("+i+").port");
					pool = new JedisPool(getPoolConfig(),ip.toString(),Integer.parseInt(port), Protocol.DEFAULT_TIMEOUT+1000);
					break;
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
			List<Object> ss = Configure.getList("redis.servers.host.ip");
	        int redislen = ss.size();
			List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
			JedisShardInfo jsi = null;
			for(int i=0;i<redislen;i++){
				String ip = Configure.getProperty("redis.servers.host("+i+").ip");
				String port = Configure.getProperty("redis.servers.host("+i+").port");
				jsi = new JedisShardInfo(ip, Integer.valueOf( port), 1000);
				//jsi = new JedisShardInfo(ip, Integer.valueOf( port), Protocol.DEFAULT_TIMEOUT+1000);
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