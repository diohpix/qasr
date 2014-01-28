package com.cj.enm.platform.dbproxy.protocol.server.http;

import io.netty.handler.codec.http.HttpMethod;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.MappedStatement.Builder;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.scripting.xmltags.DynamicSqlSource;
import org.apache.ibatis.scripting.xmltags.MixedSqlNode;
import org.apache.ibatis.scripting.xmltags.SqlNode;
import org.apache.ibatis.scripting.xmltags.TextSqlNode;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.tomcat.dbcp.dbcp.BasicDataSource;

import com.cj.enm.platform.dbproxy.db.LoadBalancer;
import com.cj.enm.platform.dbproxy.db.ReloadableSqlSesseionFactoryBean;
import com.cj.enm.platform.dbproxy.db.proxy.ProxyDataSource;
import com.cj.enm.platform.dbproxy.util.Configure;
import com.cj.enm.platform.dbproxy.util.ResponseUtil;


public class Monitor {

	public static String service(HttpMethod method,String url,Map<String, List<String>> params, String body){
			if(url.startsWith("/monitor/mybatis/list.")){
				return mybatisList(params, body);
			}else if(url.startsWith("/monitor/mapper/list.")){ // 
				return mapperList(params, body);
			}else if(url.startsWith("/monitor/mapper/sql.")){ // 
				return mapperSQL(params, body);
			}else if(url.startsWith("/monitor/mapper/delete.")){ // 
				return deleteCommand(params, body);
			}else if(url.startsWith("/monitor/mapper/update.")){ // 
				return updateCommand(params, body);
			}else if(url.startsWith("/monitor/mapper/insert.")){ // 
				return insertCommand(params, body);
			}else if(url.startsWith("/monitor/mapper/test.")){ // 
				return testCommand(params, body);
			}else if(url.startsWith("/monitor/jdbc/update.")){ // 
				return updateDS(params, body);
			}
			return null;
	}
	private static String mybatisList(Map<String, List<String>> params, String body) {
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		Map<String,Object> r = new HashMap<String,Object>();
		
		Iterator<String> names = Configure.getSQLFactoryNames();
		r.put("time",new Date());
		while(names.hasNext()){
			String name = names.next();
			LoadBalancer lb;
			try {
				lb = Configure.getLoadBalancer(name);
				List<ReloadableSqlSesseionFactoryBean> connList = lb.getSessionList();
				for (ReloadableSqlSesseionFactoryBean rs : connList) {
					Map<String,Object> data = new HashMap<String,Object>();
					SqlSessionFactory f =rs.getObject();
					BasicDataSource ds = (BasicDataSource)f.getConfiguration().getEnvironment().getDataSource();
					data.put("name", name);
					data.put("active",ds.getNumActive());
					data.put("idle", ds.getNumIdle());
					data.put("max", ds.getMaxActive());
					data.put("url",ds.getUrl());
					data.put("username",ds.getUsername());
					data.put("password",ds.getPassword());
					data.put("className",ds.getDriverClassName());
					data.put("id", ds.hashCode());
					list.add(data);
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		r.put("list", list);
		return  ResponseUtil.getJSONString(r);
	}
	private static String mapperList(Map<String, List<String>> params, String body) {
		Map<String,Object> map = ResponseUtil.convertJSONString2Map(body);
		Map<String,Object> r = new HashMap<String,Object>();
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		try {
			String name = (String) map.get("name");
			LoadBalancer lb;
			lb = Configure.getLoadBalancer(name);
			List<ReloadableSqlSesseionFactoryBean> connList = lb.getSessionList();
			for (ReloadableSqlSesseionFactoryBean rs : connList) {
				SqlSessionFactory f =rs.getObject();
				Collection<String> names = rs.getObject().getConfiguration().getMappedStatementNames();
				for (String id : names) {
					if(id.indexOf(".")>0){
						Map<String,Object> data = new HashMap<String,Object>();
						MappedStatement n = rs.getObject().getConfiguration().getMappedStatement(id);
						String ns = id.substring(0,id.lastIndexOf("."));
						String nm = id.substring(id.lastIndexOf(".")+1,id.length());
						data.put("ns",ns);
						data.put("nm",nm);
						data.put("ty",n.getSqlCommandType());
						list.add(data);
					}
					
				}
			}
		} catch (Throwable e2) {
				e2.printStackTrace();
				return "{code:500}";
		}
		r.put("list", list);
		return  ResponseUtil.getJSONString(r);
	}
	private static String mapperSQL(Map<String, List<String>> params, String body) {
		Map<String,Object> map = ResponseUtil.convertJSONString2Map(body);
		Map<String,Object> r = new HashMap<String,Object>();
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		try {
			String name = (String) map.get("name");
			String ns = (String) map.get("ns");
			String nm = (String) map.get("nm");
			String id=ns+"."+nm;
			LoadBalancer lb;
			lb = Configure.getLoadBalancer(name);
			List<ReloadableSqlSesseionFactoryBean> connList = lb.getSessionList();
			for (ReloadableSqlSesseionFactoryBean rs : connList) {
				Map<String,Object> data = new HashMap<String,Object>();
				MappedStatement n = rs.getObject().getConfiguration().getMappedStatement(id);
				String sql="";
				if(n.getSqlSource() instanceof DynamicSqlSource){
					System.out.println("dynamic");
					DynamicSqlSource source = (DynamicSqlSource) n.getSqlSource();
					Field f = DynamicSqlSource.class.getDeclaredField("rootSqlNode"); 
					f.setAccessible(true);
					SqlNode iWantThis = (SqlNode) f.get(source);
					
					List a = null;
					Field text =null;
					
					Field content = MixedSqlNode.class.getDeclaredField("contents");
					content.setAccessible(true);
					a = (List) content.get(iWantThis);
					
					for (Object SqlNode : a) {
						
						if(SqlNode instanceof TextSqlNode){
							
							text = TextSqlNode.class.getDeclaredField("text");
							text.setAccessible(true);
							System.out.println(SqlNode);
							String t = (String)text.get(SqlNode);
							sql+=t.trim();	
							
						}
						/*if(SqlNode instanceof StaticTextSqlNode){
						}else if(SqlNode instanceof TextSqlNode){
							String t = (String)text.get(SqlNode);
							sql+=t.trim();
						}*/
							

					}
				}else if(n.getSqlSource() instanceof RawSqlSource){
					System.out.println("Raw");
					RawSqlSource source = (RawSqlSource) n.getSqlSource();
					Field f = RawSqlSource.class.getDeclaredField("orgSql"); 
					sql =(String) f.get(source);
				}
				
				data.put("sql",sql);
				list.add(data);
			}
		} catch (Throwable e2) {
				e2.printStackTrace();
				return "{code:500}";
		}
		r.put("list", list);
		return  ResponseUtil.getJSONString(r);
	}
	private static String deleteCommand(Map<String, List<String>> params, String body) {
		Map<String,Object> map = ResponseUtil.convertJSONString2Map(body);
		Map<String,Object> r = new HashMap<String,Object>();
		SqlSession sess = null;
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		try {
			String name = (String) map.get("name");
			String command = (String) map.get("command");
			LoadBalancer lb;
			lb = Configure.getLoadBalancer(name);
			Map<String,Object> data = (Map<String, Object>) map.get("data");
			List<ReloadableSqlSesseionFactoryBean> connList = lb.getSessionList();
			int hashcode = ((Double)map.get("hashcode")).intValue();
			for (ReloadableSqlSesseionFactoryBean rs : connList) {
				SqlSessionFactory f =rs.getObject();
				Field mapp = Configuration.class.getDeclaredField("mappedStatements");
				mapp.setAccessible(true);
				Map<String,MappedStatement> ml = (Map<String, MappedStatement>) mapp.get(f.getConfiguration());
				ml.remove(command);
			}
			Map<String ,Object> res = new HashMap<String,Object>();
			res.put("message", "OK");
			list.add(res);
			r.put("list", list);
		} catch (Throwable e2) {
				e2.printStackTrace();
				return "{code:500}\n.\n";
		}finally{
			if(sess!=null){
				sess.close();
			}
		}
		return  ResponseUtil.getJSONString(r);
	}
	private static String updateCommand(Map<String, List<String>> params, String body) {
		System.out.println(body);
		Map<String,Object> map = ResponseUtil.convertJSONString2Map(body);
		Map<String,Object> r = new HashMap<String,Object>();
		SqlSession sess = null;
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		try {
			String name = (String) map.get("name");
			String command = (String) map.get("command");
			String sql = (String) map.get("sql");
			LoadBalancer lb;
			lb = Configure.getLoadBalancer(name);
			Map<String,Object> data = (Map<String, Object>) map.get("data");
			List<ReloadableSqlSesseionFactoryBean> connList = lb.getSessionList();
			int hashcode = ((Double)map.get("hashcode")).intValue();
			for (ReloadableSqlSesseionFactoryBean rs : connList) {
				SqlSessionFactory f =rs.getObject();
				List<SqlNode> s = new ArrayList<SqlNode>();
				s.add(new TextSqlNode(sql));
				MixedSqlNode mx = new MixedSqlNode(s);
				DynamicSqlSource d = new DynamicSqlSource(f.getConfiguration(),mx);
				MappedStatement n = rs.getObject().getConfiguration().getMappedStatement(command);
				Builder b = new  MappedStatement.Builder(f.getConfiguration(),command,d,n.getSqlCommandType());
				Field mapp = Configuration.class.getDeclaredField("mappedStatements");
				mapp.setAccessible(true);
				Map<String,MappedStatement> ml = (Map<String, MappedStatement>) mapp.get(f.getConfiguration());
				ml.remove(command);
				f.getConfiguration().addMappedStatement(b.build());
			}
			Map<String ,Object> res = new HashMap<String,Object>();
			res.put("message", "OK");
			list.add(res);
			r.put("list", list);
		} catch (Throwable e2) {
				e2.printStackTrace();
				return "{code:500}\n.\n";
		}finally{
			if(sess!=null){
				sess.close();
			}
		}
		return  ResponseUtil.getJSONString(r);
	}
	private static String insertCommand(Map<String, List<String>> params, String body) {
		Map<String,Object> map = ResponseUtil.convertJSONString2Map(body);
		Map<String,Object> r = new HashMap<String,Object>();
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		SqlSession sess = null;
		try {
			String name = (String) map.get("name");
			String command = (String) map.get("command");
			String sql = (String) map.get("sql");
			LoadBalancer lb;
			lb = Configure.getLoadBalancer(name);
			Map<String,Object> data = (Map<String, Object>) map.get("data");
			List<ReloadableSqlSesseionFactoryBean> connList = lb.getSessionList();
			int hashcode = ((Double)map.get("hashcode")).intValue();
			for (ReloadableSqlSesseionFactoryBean rs : connList) {
				SqlSessionFactory f =rs.getObject();
				List<SqlNode> s = new ArrayList<SqlNode>();
				s.add(new TextSqlNode(sql));
				MixedSqlNode mx = new MixedSqlNode(s);
				DynamicSqlSource d = new DynamicSqlSource(f.getConfiguration(),mx);
				SqlCommandType sqlType=null;
				String ty = (String) data.get("type");
				switch (ty) {
				case "SELECT":
					sqlType = SqlCommandType.SELECT;
					break;
				case "INSERT":
					sqlType = SqlCommandType.INSERT;
					break;
				case "UPDATE":
					sqlType = SqlCommandType.UPDATE;
					break;
				case "DELETE":
					sqlType = SqlCommandType.DELETE;
					break;
				default:
					break;
				}
				Builder b = new  MappedStatement.Builder(f.getConfiguration(),command,d,sqlType);
				Field mapp = Configuration.class.getDeclaredField("mappedStatements");
				mapp.setAccessible(true);
				/*Map<String,MappedStatement> ml = (Map<String, MappedStatement>) mapp.get(f.getConfiguration());
				ml.remove(command);*/
				f.getConfiguration().addMappedStatement(b.build());
			}
			Map<String ,Object> res = new HashMap<String,Object>();
			res.put("message", "OK");
			list.add(res);
			r.put("list", list);
		} catch (Throwable e2) {
				e2.printStackTrace();
				return "{code:500}\n.\n";
		}finally{
			if(sess!=null){
				sess.close();
			}
		}
		return  ResponseUtil.getJSONString(r);
	}
	private static String updateDS(Map<String, List<String>> params, String body) {
		Map<String,Object> map = ResponseUtil.convertJSONString2Map(body);
		Map<String,Object> r = new HashMap<String,Object>();
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		String name = (String) map.get("name");
		int hashcode = ((Double)map.get("id")).intValue();
		LoadBalancer lb;
		try {
			lb = Configure.getLoadBalancer(name);
			List<ReloadableSqlSesseionFactoryBean> connList = lb.getSessionList();
			for (ReloadableSqlSesseionFactoryBean rs : connList) {
				SqlSessionFactory f =rs.getObject();
				ProxyDataSource ds = (ProxyDataSource)f.getConfiguration().getEnvironment().getDataSource();
				if(hashcode == ds.hashCode()){
					String username = (String) map.get("username");
					String password = (String) map.get("password");
					String className = (String) map.get("className");
					String url = (String) map.get("url");
					ds.setUsername(username);
	            	ds.setPassword(password);
	            	ds.setDriverClassName(className);
	            	ds.setDefaultAutoCommit(ds.getDefaultAutoCommit());
	            	ds.setUrl(url);
	            	ds.reset();
				}
			}
			Map<String ,Object> res = new HashMap<String,Object>();
			res.put("message", "OK");
			list.add(res);
			r.put("list", list);
		} catch (Throwable e2) {
				e2.printStackTrace();
				return "{code:500}";
		}
		return "{code:200}";
	}
	
	private static String testCommand(Map<String, List<String>> params, String body) {
		Map<String,Object> map = ResponseUtil.convertJSONString2Map(body);
		Map<String,Object> r = new HashMap<String,Object>();
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		SqlSession sess = null;
		try {
			String name = (String) map.get("name");
			String command = (String) map.get("command");
			LoadBalancer lb;
			lb = Configure.getLoadBalancer(name);
			Map<String,Object> data = (Map<String, Object>) map.get("data");
			List<ReloadableSqlSesseionFactoryBean> connList = lb.getSessionList();
			int hashcode = ((Double)map.get("hashcode")).intValue();
			for (ReloadableSqlSesseionFactoryBean rs : connList) {
				SqlSessionFactory f =rs.getObject();
				ProxyDataSource ds = (ProxyDataSource)f.getConfiguration().getEnvironment().getDataSource();
				if(hashcode == ds.hashCode()){
					sess = f.openSession();
					List<Map> result = sess.selectList(command, data);
					if(result.size()>0){
						result.remove(0);
						r.put("list",result.get(0));
					}else{
						r.put("list",result);
					}
				}
			}
		} catch (Throwable e) {
				Map<String,Object> res = new HashMap<String,Object>();
				res.put("code", "E0004");
				if(e.getCause()!=null){
					res.put("msg", e.getCause().getMessage());
				}else{
					res.put("msg", e.toString());
				}
				List<Map> l = new ArrayList<Map>();
				l.add(res);
				r.put("list",l);
		}finally{
			if(sess!=null){
				sess.close();
			}
		}
		return  ResponseUtil.getJSONString(r);
	}
}
