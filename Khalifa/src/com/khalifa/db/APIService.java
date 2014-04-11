
package com.khalifa.db;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import com.google.common.base.Throwables;
import com.khalifa.cache.redis.JedisConnection;
import com.khalifa.db.proxy.ProxySqlSession;
import com.khalifa.process.State;
import com.khalifa.protocol.QueryProtocol.Response;
import com.khalifa.util.CommonData;
import com.khalifa.util.Configure;
import com.khalifa.util.ResponseUtil;
import com.khalifa.util.UK;



public class APIService {
	private final static Logger logger = LoggerFactory.getLogger(APIService.class);
	private final static Logger slowqueryLogger = LoggerFactory.getLogger("SLOWQUERY");
	private static int slow_query_time = CommonData.slow_query_time;
	
	public static SqlSession getSession(String dbname) throws Exception{
		SqlSessionFactory factory =  CommonData.getSQLFactory(dbname);
		return  factory.openSession();	
	}
	public static SqlSession getBatchSession(String dbname) throws Exception{
		SqlSessionFactory factory =  CommonData.getSQLFactory(dbname);
		return  factory.openSession(ExecutorType.BATCH);	
	}
	public static Object transactionQuery(ProxySqlSession sess, String SQL,Map<String,Object> where,State state) throws Exception{
		Response.Builder res =null;
		long time = System.currentTimeMillis();
		List<List<Object>> objs =null;
		List<Object> obj =null;
		MappedStatement ms = sess.getConfiguration().getMappedStatement(SQL);
			if(ms.getSqlCommandType() == SqlCommandType.SELECT){ // SELECT일경우
				objs = sess.selectList(ms, where);
				sess.clearCache();
			}else{
				objs = new ArrayList<List<Object>>();
				obj = new ArrayList<Object>();
				List<Map<String, Object>> l = cudProcess(sess, SQL, where, obj,	ms);
				obj.add(l);
				objs.add(obj);
			}
			if(ms.getStatementType() == StatementType.CALLABLE){
				outputParamProcess(where, state, ms);
			}

		long elap = System.currentTimeMillis()-time;
		if(elap > slow_query_time){
			slowqueryLogger.info("{} [ {} ] : time : {}", SQL, where, (elap));
		}
		res = Response.newBuilder();
		for (List<Object> object : objs) {
			Response.Data.Builder data = UK.convertObject2Response(object);
			res.addData(data);
		}
		res.setCode(200);
		return res;
	}
	public static Object batchQuery(ProxySqlSession sess, String SQL,List<Map<String,Object>> param,State state,boolean tx) {
		Response.Builder res = Response.newBuilder();
		try{
			long time = System.currentTimeMillis();
			res = Response.newBuilder();
			MappedStatement ms = sess.getConfiguration().getMappedStatement(SQL);
			Object [] meta = null;
			List<Object> obj =new ArrayList<Object>();
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
			List<Map<String,Object>> line = new ArrayList<Map<String,Object>>();
			for (Map<String,Object> object : param) {
				if(ms.getSqlCommandType() == SqlCommandType.INSERT){ // insert
					sess.insert(SQL, object);
				}else if(ms.getSqlCommandType() == SqlCommandType.UPDATE){ // update 
					sess.update(SQL, object);
				}else if(ms.getSqlCommandType() == SqlCommandType.DELETE){ // delete
					sess.delete(SQL, object);
				}
			}
			List<BatchResult> result = sess.flushStatements();
			String genkey =null;
			if(ms.getSqlCommandType() == SqlCommandType.INSERT){ // insert
				if(ms.getKeyProperties()!=null){
					genkey =ms.getKeyProperties()[0];
				}
			}
			for (BatchResult batchResult : result) {
				int [] update  = batchResult.getUpdateCounts();
				int count=0;
				for (int i : update) {
					Map<String, Object> data = new HashMap<String,Object>();
					data.put("__count__", i);
					if(genkey!=null){
						List<Object> pm = batchResult.getParameterObjects();
						Map<String,Object> qq =(Map<String,Object>) pm.get(count++);
						if(qq.containsKey(genkey)){
							data.put("__insert__id__", qq.get(genkey));
						}
					}
					line.add(data);		
				}
			}
			long elap = System.currentTimeMillis()-time;
			if(elap > slow_query_time){
				slowqueryLogger.info("{} [ {} ] : time : {}", SQL, "_BATCH_JOB_", (elap));
			}
			obj.add(line);
			Response.Data.Builder d = UK.convertObject2Response(obj);
			res.addData(d);
			res.setCode(200);
			if(!tx){ // if this batch job is a partial of transaction . do not commit; 
				sess.commit();
			}
		}catch(Exception e){
			if(!tx){
				if(sess!=null){
					sess.rollback();
				}
			}
			throw e;
		}finally{
			if(!tx){
				if(sess!=null){
					sess.close();
				}
			}
		}
		return res;
	}

	private static List<Map<String, Object>> cudProcess(ProxySqlSession sess,String SQL, Map<String, Object> where, List<Object> obj,MappedStatement ms) throws SQLException {
		int rtn = 0;
		sess.getConnection().setReadOnly(false);
		long lastSeq=0;
		String genkey =null;
		if(ms.getSqlCommandType() == SqlCommandType.INSERT){ // insert
			rtn = sess.insert(ms, where);
			if(ms.getKeyProperties()!=null){
				genkey =ms.getKeyProperties()[0];
				if(where.get(genkey)!=null){
					lastSeq = ((Long)where.get(genkey)).longValue();
				}
			}
		}else if(ms.getSqlCommandType() == SqlCommandType.UPDATE){ // update 
			rtn = sess.update(SQL, where);
		}else if(ms.getSqlCommandType() == SqlCommandType.DELETE){ // delete
			rtn = sess.delete(SQL, where);
		}
		
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
		if(sess.getExcutorType()== ExecutorType.BATCH){ // transaction 일 경우다 .
			List<BatchResult> result = sess.flushStatements();
			for (BatchResult batchResult : result) {
				int [] update  = batchResult.getUpdateCounts();
				Map<String, Object> data = new HashMap<String,Object>();
				if(where.get(genkey)!=null){
					lastSeq = ((Long)where.get(genkey)).longValue();
					data.put("__insert__id__", lastSeq);
				}
				for (int i : update) {
					data.put("__count__", i);
					l.add(data);
				}
			}
		}else{
			Map<String, Object> data = new HashMap<String,Object>();
			data.put("__count__", rtn);
			if(ms.getSqlCommandType() == SqlCommandType.INSERT){
				data.put("__insert__id__", lastSeq);
			}
			l.add(data);
		}
		return l;
	}
	private static void outputParamProcess(Map<String, Object> where,State state, MappedStatement ms) {
		final List<ParameterMapping> parameterMappings = ms.getBoundSql(where).getParameterMappings();
		List<Object> actual = new ArrayList<Object>();
		Object[] metaData = new Object[1];
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		int c=0;
		for (int i = 0; i < parameterMappings.size(); i++) {
		  final ParameterMapping parameterMapping = parameterMappings.get(i);
		  if (parameterMapping.getMode() == ParameterMode.OUT || parameterMapping.getMode() == ParameterMode.INOUT) {
				String columnName = parameterMapping.getProperty();
				String javatype = parameterMapping.getJavaType().getName();
				Map<String, String> columnMeta = new LinkedHashMap<String,String>();
				columnMeta.put("columnName", columnName);
				columnMeta.put("columnLabel", columnName);
				columnMeta.put("columnType", javatype);
				metaData[c++] = columnMeta;

				Map<String, Object> col = new HashMap<String, Object>();
				col.put(columnName, where.get(columnName));
				list.add(col);
		  }
		}
		if(c>0){
			actual.add(metaData);
			actual.add(list);
			Response.Data.Builder rs = UK.convertObject2Response(actual);
			Response.Builder b = Response.newBuilder();
			b.setCode(200);
			b.addData(rs);
			state.setOutputParam(b);
		}
	}	
	public static Object query(String dbname,int type,String SQL,Map<String,Object> where,String _where ,int expireTime,State state) throws Exception{
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
			select = getRedisCache(_where,CACHE_KEY,SQL);
			if(select !=null) return select;
		}
		long time = System.currentTimeMillis();
		List<List<Object>> objs =null;
		List<Object> obj =null;
		ProxySqlSession sess =null;
		try{
			sess = (ProxySqlSession) getSession(dbname);
			MappedStatement ms = sess.getConfiguration().getMappedStatement(SQL);
			if(ms.getSqlCommandType()==SqlCommandType.SELECT){ // SELECT일경우 
				sess.getConnection().setReadOnly(true);
				objs = sess.selectList(ms, where);
			}else{
				objs = new ArrayList<List<Object>>();
				obj = new ArrayList<Object>();
				List<Map<String, Object>> l = cudProcess(sess,SQL, where, obj,	 ms);
				sess.commit();
				obj.add(l);
				objs.add(obj);
			}
			if(ms.getStatementType() == StatementType.CALLABLE){
				outputParamProcess(where, state, ms);
			}
			sess.close();
		}catch(Exception e){
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
		res = Response.newBuilder();
		res.setCode(200);
		ResponseUtil.addList2Response(res, objs);
        if(type==1){
    		if( res.getDataCount() > 0){
				addRedisCache(_where, expireTime,select, res,CACHE_KEY);
    		}
        }
		return res;
	}
	public static Object readOnlyQuery(ProxySqlSession sess,String dbname,int type,String SQL,Map<String,Object> where,String _where ,int expireTime,State state) throws Exception{
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
			select = getRedisCache(_where,CACHE_KEY,SQL);
			if(select !=null) return select;
		}
		long time = System.currentTimeMillis();
		List<List<Object>> objs =null;
		try{
			MappedStatement ms = sess.getConfiguration().getMappedStatement(SQL);
			if(ms.getSqlCommandType()==SqlCommandType.SELECT ){ // SELECT일경우 
				sess.getConnection().setReadOnly(true);
				objs = sess.selectList(ms, where);
			}else{
				throw new Exception( "only select statment is allowed and callablestatment not allowed");
			}
		}catch(Exception e){
			e.printStackTrace();
			throw e;
		}finally{
		}
		long elap = System.currentTimeMillis()-time;
		if(elap > slow_query_time){
			slowqueryLogger.info("{} [ {} ] : time : {}", SQL, where, (elap));
		}
		res = Response.newBuilder();
		res.setCode(200);
		ResponseUtil.addList2Response(res, objs);
        if(type==1){
    		if( res.getDataCount() > 0){
				addRedisCache(_where, expireTime, select, res,CACHE_KEY);
    		}
        }
		return res;
	}
	private static byte[]  getRedisCache(String _where,String CACHE_KEY,String SQL){
		byte[]select=null;
		ShardedJedis jedis = null;
		ShardedJedisPool pool=null;
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
		return null;
	}
	private static void addRedisCache(String _where, int expireTime,byte[] select,	Response.Builder res, String CACHE_KEY) {
		ShardedJedisPool pool=null;
		ShardedJedis jedis=null;
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
			logger.error(Throwables.getStackTraceAsString(e));
			if(pool!=null){
				JedisConnection.setCheckJEDIS();
			}
		}finally{
			if(pool!=null && jedis!=null){
				pool.returnResource(jedis);
			}
		}
	}
}
