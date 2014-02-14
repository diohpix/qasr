
package com.khalifa.db;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.StatementType;
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
import com.khalifa.util.UK;



public class APIService {
	private final static Logger logger = LoggerFactory.getLogger(APIService.class);
	private final static Logger slowqueryLogger = LoggerFactory.getLogger("SLOWQUERY");
	private static int slow_query_time = CommonData.slow_query_time;
	
	public static SqlSession getSession(String dbname) throws Exception{
		SqlSessionFactory factory =  CommonData.getSQLFactory(dbname);
		return  factory.openSession();	
	}
	public static Object transactionQuery(ProxySqlSession sess, String SQL,Map<String,Object> where,State state) throws Exception{
		Response.Builder res =null;
		long time = System.currentTimeMillis();
		List<Object> obj =null;
		MappedStatement ms = sess.getConfiguration().getMappedStatement(SQL);
			if(ms.getSqlCommandType() == SqlCommandType.SELECT){ // SELECT일경우
				obj = sess.selectList(ms, where);
				sess.clearCache();
			}else{
				obj = new ArrayList<Object>();
				List<Map<String, Object>> l = cudProcess(sess, SQL, where, obj,	ms);
				obj.add(l);
			}
			if(ms.getStatementType() == StatementType.CALLABLE){
				outputParamProcess(where, state, ms);
			}

		long elap = System.currentTimeMillis()-time;
		if(elap > slow_query_time){
			slowqueryLogger.info("{} [ {} ] : time : {}", SQL, where, (elap));
		}
		res = UK.convertObject2Response(obj);
		res.setCode(200);
		return res;
	}
	private static List<Map<String, Object>> cudProcess(ProxySqlSession sess,String SQL, Map<String, Object> where, List<Object> obj,MappedStatement ms) throws SQLException {
		int rtn = 0;
		sess.getConnection().setReadOnly(false);
		long lastSeq=0;
		if(ms.getSqlCommandType() == SqlCommandType.INSERT){ // insert
			rtn = sess.insert(ms, where);
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
		return l;
	}
	private static void outputParamProcess(Map<String, Object> where,
			State state, MappedStatement ms) {
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
			  /*if (ResultSet.class.equals(parameterMapping.getJavaType())) {
		      		//handleRefCursorOutputParameter((ResultSet) cs.getObject(i + 1), parameterMapping, metaParam);
		    	} else {
		      final TypeHandler<?> typeHandler = parameterMapping.getTypeHandler();
		      		//metaParam.setValue(parameterMapping.getProperty(), typeHandler.getResult(cs, i + 1));
		    	}*/
		  }
		}
		actual.add(metaData);
		actual.add(list);
		Response.Builder rs = UK.convertObject2Response(actual);
		state.setOutputParam(rs);
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
				List<Map<String, Object>> l = cudProcess(sess,SQL, where, obj,	 ms);
				sess.commit();
				obj.add(l);
			}
			if(ms.getStatementType() == StatementType.CALLABLE){
				outputParamProcess(where, state, ms);
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
		List<Object> obj =null;
		try{
			sess = (ProxySqlSession) getSession(dbname);
			MappedStatement ms = sess.getConfiguration().getMappedStatement(SQL);
			if(ms.getSqlCommandType()==SqlCommandType.SELECT && ms.getStatementType() != StatementType.CALLABLE){ // SELECT일경우 
				sess.getConnection().setReadOnly(true);
				obj = sess.selectList(ms, where);
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
		res = UK.convertObject2Response(obj);
		res.setCode(200);
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
			e.printStackTrace();
			logger.error(Throwables.getStackTraceAsString(e));
		}finally{
			if(pool!=null && jedis!=null){
				pool.returnResource(jedis);
			}
		}
	}
}
