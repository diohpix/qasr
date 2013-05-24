package com.qasr.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.qasr.cache.redis.JedisConnection;
import com.qasr.protocol.QueryProtocol.Response;
import com.qasr.util.Configure;
import com.qasr.util.UK;


import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;



public class APIService {
	private final static Logger logger = LoggerFactory.getLogger(APIService.class);
	//private static SqlSessionFactory f = CommonObject.context.getBean("mainSessionFactory", SqlSessionFactory.class);
	private static int slow_query_time = Configure.getIntProperty("slow_query_time");
	
	public static SqlSession getSession(String dbname) throws Exception{
		SqlSessionFactory factory =  Configure.getSQLFactory(dbname);
		return  factory.openSession();	
	}
	public static Object transactionQuery(SqlSession sess, int type,String SQL,Map<String,Object> where ) throws Exception{
		Response.Builder res =null;
		long time = System.currentTimeMillis();
		List<Object> obj =null;
			if(type==1){ // SELECT일경우
				obj = sess.selectList(SQL, where);
				sess.clearCache();
			}else{
				obj = new ArrayList<Object>();
				int rtn = 0;
				sess.getConnection().setReadOnly(false);
				long lastSeq=0;
				if(type==2){ // insert
					rtn = sess.insert(SQL, where);
					if(where.get("__last_insert_id__")!=null){
						lastSeq = ((Long)where.get("__last_insert_id__")).longValue();
					}
				}else if(type==3){ // update 
					rtn = sess.update(SQL, where);
				}else if(type ==4){ // delete
					rtn = sess.delete(SQL, where);
				}
				sess.commit();
				
				Object [] meta = new Object[1];
				if(type==2){
					meta = new Object[2];
				}else{
					meta = new Object[1];
				}
				Map<String, String> metaData = new HashMap<String,String>();
				metaData.put("columnLabel","__count__");
				metaData.put("columnName","__count__");
				metaData.put("columnType","java.lang.Integer");
				meta[0]=metaData;
				if(type==2){
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
				if(type==2){
					data.put("__insert__id__", lastSeq);
				}
				l.add(data);
				sess.commit();
				obj.add(l);
			}
		long elap = System.currentTimeMillis()-time;
		if(elap > slow_query_time){
			logger.warn("SLOW QUERY  COMMAND {} [ {} ] : LAP {} ", SQL, where, (elap));
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
		logger.debug("EXPIRE");
		if(type==1 && expireTime > 0){
			try {
				pool = JedisConnection.getShardedInstance();
				if(pool!=null){
					jedis = pool.getResource();
					if(where ==null){
						select = jedis.get(SQL.getBytes());
					}else{
						select = jedis.hget(SQL.getBytes(), _where.getBytes());
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
		SqlSession sess =null;
		try{
			sess = getSession(dbname);
			if(type==1){ // SELECT일경우 
				sess.getConnection().setReadOnly(true);
				obj = sess.selectList(SQL, where);
			}else{
				obj = new ArrayList<Object>();
				int rtn = 0;
				long lastSeq=0;
				sess.getConnection().setReadOnly(false);
				if(type==2){ // insert
					rtn = sess.insert(SQL, where);
					if(where.get("__last_insert_id__")!=null){
						lastSeq = ((Long)where.get("__last_insert_id__")).longValue();
					}
				/*	List<Object> tobj = sess.selectList("__insert__id__");
					for (Object object : tobj) {
						//Integer i = ((Map<String,Integer>)obj.get(1)).get("__insert__id__");
						Long i = (Long) ((List<Map<String,Object>>)tobj.get(1)).get(0).get("__insert__id__");
						lastSeq = i;
					}*/
				}else if(type==3){ // update 
					rtn = sess.update(SQL, where);
				}else if(type ==4){ // delete
					rtn = sess.delete(SQL, where);
				}
				Object [] meta = new Object[1];
				if(type==2){
					meta = new Object[2];
				}else{
					meta = new Object[1];
				}
				Map<String, String> metaData = new HashMap<String,String>();
				metaData.put("columnLabel","__count__");
				metaData.put("columnName","__count__");
				metaData.put("columnType","java.lang.Integer");
				meta[0]=metaData;
				if(type==2){
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
				if(type==2){
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
			logger.warn("SLOW QUERY  COMMAND {} [ {} ] : LAP {} ", SQL, _where, (elap));
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
				        	jedis.hset(SQL.getBytes(), _where.getBytes(), select);
				        }else{
				        	jedis.set(SQL.getBytes(), select);
				        }
				        if(expireTime >-1){
				        	jedis.expire(SQL.getBytes(),expireTime);
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
