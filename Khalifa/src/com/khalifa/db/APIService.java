
package com.khalifa.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import com.google.common.base.Throwables;
import com.khalifa.cache.redis.JedisConnection;
import com.khalifa.db.proxy.ProxySqlSession;
import com.khalifa.protocol.QueryProtocol.Response;
import com.khalifa.util.Configure;
import com.khalifa.util.UK;



public class APIService {
	private final static Logger logger = LoggerFactory.getLogger(APIService.class);
	private final static Logger slowqueryLogger = LoggerFactory.getLogger("SLOWQUERY");
	private static int slow_query_time = Configure.getIntProperty("slow_query_time");
	
	public static SqlSession getSession(String dbname) throws Exception{
		SqlSessionFactory factory =  Configure.getSQLFactory(dbname);
		return  factory.openSession();	
	}
	public static Object transactionQuery(ProxySqlSession sess, String SQL,Map<String,Object> where ) throws Exception{
		Response.Builder res =null;
		long time = System.currentTimeMillis();
		List<Object> obj =null;
		MappedStatement ms = sess.getConfiguration().getMappedStatement(SQL);
			if(ms.getSqlCommandType() == SqlCommandType.SELECT){ // SELECT일경우
				obj = sess.selectList(ms, where);
				sess.clearCache();
			}else{
				obj = new ArrayList<Object>();
				int rtn = 0;
				sess.getConnection().setReadOnly(false);
				long lastSeq=0;
				if(ms.getSqlCommandType() == SqlCommandType.INSERT){ // insert
					rtn = sess.insert(SQL, where);
					if(ms.getKeyProperties()!=null){
						String genkey =ms.getKeyProperties()[0];
						if(where.get(genkey)!=null){
							lastSeq = ((Long)where.get(genkey)).longValue();
						}
					}
				}else if(ms.getSqlCommandType() == SqlCommandType.UPDATE){ // update 
					rtn = sess.update(SQL, where);
				}else if(ms.getSqlCommandType() == SqlCommandType.DELETE){ // delete
					rtn = sess.delete(SQL, where);
				}
				sess.commit();
				
				Object [] meta = new Object[1];
				if(ms.getSqlCommandType() == SqlCommandType.INSERT){
					meta = new Object[2];
				}else{
					meta = new Object[1];
				}
				Map<String, String> metaData = new HashMap<String,String>();
				metaData.put("columnLabel","__count__");
				metaData.put("columnName","__count__");
				metaData.put("columnType","java.lang.Integer");
				meta[0]=metaData;
				if(ms.getSqlCommandType() == SqlCommandType.INSERT){
					Map<String, String> insData = new HashMap<String,String>();
					insData.put("columnLabel","__insert__id__");
					insData.put("columnName","__insert__id__");
					insData.put("columnType","java.lang.Long");
					meta[1]=insData;
				}
				obj.add(meta);
				List<Map<String,Object>> l = new  ArrayList<Map<String,Object>>();
				Map<String, Object> data = new HashMap<String,Object>();
				data.put("__count__", Integer.valueOf(rtn));
				if(ms.getSqlCommandType() == SqlCommandType.INSERT){
					data.put("__insert__id__", lastSeq);
				}
				l.add(data);
				sess.commit();
				obj.add(l);
			}
		long elap = System.currentTimeMillis()-time;
		if(elap > slow_query_time){
			slowqueryLogger.info("{} [ {} ] : time : {}", SQL, where, (elap));
		}
		res = UK.convertObject2Response(obj);
		res.setCode(200);
		return res;
	}	
	public static Object query(String dbname,int type,String SQL,Map<String,Object> where,String _where ,int expireTime) throws Exception{
		ShardedJedisPool pool = null;
		ShardedJedis jedis = null;
		byte[] select = null;
		Response.Builder res =null;
		String CACHE_KEY=SQL;
		String CK = null;
		if(where !=null){
			CK = (String)where.get("#_CK_#");
			if(CK!=null){
				CACHE_KEY+=CK;
			}
		}
		if(type==1 && expireTime > 0){
			try {
				pool = JedisConnection.getShardedInstance();
				if(pool!=null){
					jedis = pool.getResource();
					if(_where ==null){
						select = jedis.get(CACHE_KEY.getBytes());
					}else{
						select = jedis.hget(CACHE_KEY.getBytes(), _where.getBytes());
					}
					if(select!=null) {
						logger.debug("FROM CACHE {} {}", SQL, _where);
						return select;
					}
				}
			}catch(Exception e){
				e.printStackTrace();
				if(pool!=null){
					JedisConnection.setCheckJEDIS();
				}
			} finally {
				if (pool!=null && jedis != null)
					pool.returnResource(jedis);
			}
		}
		long time = System.currentTimeMillis();
		List<Object> obj =null;
		ProxySqlSession sess =null;
		try{
			sess = (ProxySqlSession) getSession(dbname);
			MappedStatement ms = sess.getConfiguration().getMappedStatement(SQL);
			if(ms.getSqlCommandType()==SqlCommandType.SELECT){ // SELECT일경우 
				sess.getConnection().setReadOnly(true);
				obj = sess.selectList(ms, where);
			}else{
				obj = new ArrayList<Object>();
				int rtn = 0;
				long lastSeq=0;
				sess.getConnection().setReadOnly(false);
				if(ms.getSqlCommandType()==SqlCommandType.INSERT){ // insert
					rtn = sess.insert(ms, where);
					if(ms.getKeyProperties()!=null){
						String genkey = ms.getKeyProperties()[0];
						if(where.get(genkey)!=null){
							lastSeq = ((Long)where.get(genkey)).longValue();
						}
					}
				}else if(ms.getSqlCommandType()==SqlCommandType.UPDATE){ // update 
					rtn = sess.update(SQL, where);
				}else if(ms.getSqlCommandType()==SqlCommandType.DELETE){ // delete
					rtn = sess.delete(SQL, where);
				}
				Object [] meta = new Object[1];
				if(ms.getSqlCommandType()==SqlCommandType.INSERT){
					meta = new Object[2];
				}else{
					meta = new Object[1];
				}
				Map<String, String> metaData = new HashMap<String,String>();
				metaData.put("columnLabel","__count__");
				metaData.put("columnName","__count__");
				metaData.put("columnType","java.lang.Integer");
				meta[0]=metaData;
				if(ms.getSqlCommandType()==SqlCommandType.INSERT){
					Map<String, String> insData = new HashMap<String,String>();
					insData.put("columnLabel","__insert__id__");
					insData.put("columnName","__insert__id__");
					insData.put("columnType","java.lang.Long");
					meta[1]=insData;
				}
				obj.add(meta);
				List<Map<String,Object>> l = new  ArrayList<Map<String,Object>>();
				Map<String, Object> data = new HashMap<String,Object>();
				data.put("__count__", Integer.valueOf(rtn));
				if(ms.getSqlCommandType()==SqlCommandType.INSERT){
					data.put("__insert__id__", lastSeq);
				}
				l.add(data);
				sess.commit();
				obj.add(l);
			}
			sess.close();
		}catch(Exception e){
			e.printStackTrace();
			if(type>1){
				sess.rollback();
			}
			throw e;
		}finally{
			if(sess!=null){
				sess.close();
			}
		}
		long elap = System.currentTimeMillis()-time;
		if(elap > slow_query_time){
			slowqueryLogger.info("{} [ {} ] : time : {}", SQL, where, (elap));
		}
		res = UK.convertObject2Response(obj);
		res.setCode(200);
        if(type==1){
    		if( res.getDataCount() > 0){
				try{
					pool = JedisConnection.getShardedInstance();
					if(pool!=null){
						if(Configure.useCacheGzip){
			    			byte [] _select = res.build().toByteArray();
			    	        try {
			    				select = UK.compress(_select);
			    			} catch (IOException e) {
			    				e.printStackTrace();
			    			}
			            }else{
			            	select = res.build().toByteArray();
			            }
						jedis = pool.getResource();
				        if(_where !=null){
				        	jedis.hset(CACHE_KEY.getBytes(), _where.getBytes(), select);
				        }else{
				        	jedis.set(CACHE_KEY.getBytes(), select);
				        }
				        if(expireTime >-1){
				        	jedis.expire(CACHE_KEY.getBytes(),expireTime);
				        }
					}
				}catch(Exception e){
					e.printStackTrace();
					logger.error(Throwables.getStackTraceAsString(e));
				}finally{
					if(pool!=null && jedis!=null){
						pool.returnResource(jedis);
					}
				}
    		}
        }
		return res;
	}
	
}
