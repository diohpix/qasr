package com.khalifa.db;

import gudusoft.gsqlparser.EDbVendor;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.khalifa.util.Configure;


public class ReloadableSqlSesseionFactoryBean extends SqlSessionFactoryBean	implements DisposableBean {

	private final Logger log = LoggerFactory.getLogger(ReloadableSqlSesseionFactoryBean.class);
	private SqlSessionFactory proxy;
	private int interval = Configure.getIntProperty("mapper_check_interval");
	private Timer timer;
	private TimerTask task;
	private Resource[] mapperLocations;
	private String vendor;
	/**

	 * 파일 감시 쓰레드가 실행중인지 여부.

	 */
	public ReloadableSqlSesseionFactoryBean(String vendor){
		this.vendor = vendor;
	}
	private boolean running = false;
	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	private final Lock r = rwl.readLock();
	private final Lock w = rwl.writeLock();
	/*
	public ReloadableSqlSesseionFactoryBean(SqlSessionFactoryBuilder sb){
		super.setSqlSessionFactoryBuilder(sb);
	}*/
	public void setMapperLocations(Resource[] mapperLocations) {
		super.setMapperLocations(mapperLocations);
		this.mapperLocations = mapperLocations;
	
		/*
		String [] arr = new String[mapperLocations.length];
		for(int i = 0 ; i < arr.length ; i++){
			try {
				Resource r = mapperLocations[i];
				File f = r.getFile();
				arr[i] = f.getAbsolutePath();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			parseSQLMapper(arr);
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}




	public void setInterval(int interval) {
		this.interval = interval;
	}



	/**

	 *

	 * @throws Exception

	 */

	public void refresh() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("refreshing sqlMapClient.");
		}
		w.lock();
		try {
			List<Resource> list = new ArrayList<Resource>();
			 for(int j = 0 ; j < mapperLocations.length ; j++){
				Resource r = mapperLocations[j];
				System.out.println(r.getFile().getAbsolutePath());
				File f = r.getFile();
				if(f.exists()){
					Resource rs =  new  FileSystemResource(f);
					list.add(rs);
				}
			 }
			this.mapperLocations = list.toArray(new Resource[list.size()]);
			list.clear();
			super.setMapperLocations(mapperLocations);	
			super.afterPropertiesSet();
		} finally {
			w.unlock();
		}
	}
	private static List<String> finishedFile = new ArrayList<String>();
	private static DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	private static DocumentBuilder builder = null;
	private static Pattern  p = Pattern.compile("([$#]\\{((\\p{Alnum}|.+?))\\})");
	
	public static String convertSQL(String sql) throws Exception{
		Matcher m = null;
		m = p.matcher(sql);
		StringBuffer sb = new StringBuffer();
		int lastP=0;
		while(m.find())
		{
			if(m.group().startsWith("#")){
				m.appendReplacement(sb, " ?");	
			}else{
				String l =sql.substring(lastP,m.start());
				if(l.toLowerCase().indexOf("top ")>-1){
					m.appendReplacement(sb, "0");
				}else{
					m.appendReplacement(sb, "desc");
				}
				lastP = m.end();
			}
		}
		m.appendTail(sb);
		return sb.toString();
	}
	
	public void parseSQLMapper(String [] files) throws Exception{
		if(builder==null){
			 builder = factory.newDocumentBuilder();
		}
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		for (String path : files) {
			if(finishedFile.contains(path)) return;
			String p =path;// path.substring(path.indexOf("[")+1,path.lastIndexOf("]"));
			Document doc = builder.parse(p);
			NodeList newslist = doc.getElementsByTagName("mapper");
			for (int loop = 0; loop < newslist.getLength(); loop++) {
				Node node = newslist.item(loop);
				String ns = node.getAttributes().getNamedItem("namespace").getNodeValue().trim();
				NodeList q = node.getChildNodes();
				for(int i=0;i<q.getLength();i++){
					Map<String,String> info = new HashMap<String,String>();
					Node sql = q.item(i);
					Node nid = sql.getAttributes() !=null ? sql.getAttributes().getNamedItem("id") : null;
					if(nid!=null){
						String id = ns+"."+sql.getAttributes().getNamedItem("id").getNodeValue();
						info.put("sqlType", sql.getNodeName());
						info.put("command",id);
						NodeList item  = sql.getChildNodes();
						info.put("sql",sql.getTextContent().trim());
						String osql = info.get("sql");
						osql = convertSQL(osql);
						EDbVendor vendor = EDbVendor.dbvmssql;
						if(this.vendor.indexOf("mysql")>-1){
							vendor = EDbVendor.dbvmysql;
						}else if(this.vendor.indexOf("sqlserver")>-1){
							vendor = EDbVendor.dbvmssql;
						}else if(this.vendor.indexOf("oracle")>-1){
							vendor = EDbVendor.dbvoracle;
						}else{
							vendor = EDbVendor.dbvansi;
						}
						System.out.println("scan "+this.vendor);
						//scantable scan = new scantable( osql, vendor );
						//System.out.print( scan.getScanResult( ) );
						
					}
				}
			}
			finishedFile.add(path);
		}
	}

	/**

	 * 싱글톤 멤버로 SqlMapClient 원본 대신 프록시로 설정하도록 오버라이드.

	 */

	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		setRefreshable();
	}



	private void setRefreshable() {
		proxy = (SqlSessionFactory) Proxy.newProxyInstance(
				SqlSessionFactory.class.getClassLoader(),
				new Class[] { SqlSessionFactory.class },
				new InvocationHandler() {
					@Override
					public Object invoke(Object proxy, Method method, Object[] args)
							throws Throwable {
						return method.invoke(getParentObject(), args);
					}
				});

		task = new TimerTask() {
			private Map<Resource, Long> map = new HashMap<Resource, Long>();
			public void run() {
				if (isModified()) {
					try {
						refresh();
					} catch (Exception e) {
						log.error("caught exception", e);
					}
				}
			}
			private boolean isModified() {
				boolean retVal = false;
				w.lock();
				try{
					if (mapperLocations != null) {
						for (int i = 0; i < mapperLocations.length; i++) {
							Resource mappingLocation = mapperLocations[i];
							retVal |= findModifiedResource(mappingLocation);
						}
					
					}
				}catch(Exception e){
					
				}finally{
					w.unlock();
				}
				return retVal;
			}
			private boolean findModifiedResource(Resource resource) {
				boolean retVal = false;
				//List<String> modifiedResources = new ArrayList<String>();
				try {
					if(resource==null || !resource.exists()) return false;
					long modified = resource.lastModified();
					if (map.containsKey(resource)) {
						long lastModified = ((Long) map.get(resource)).longValue();
						if (lastModified != modified) {
							map.put(resource, new Long(modified));
					//		modifiedResources.add(resource.getDescription());
							retVal = true;
						}
					} else {
						map.put(resource, new Long(modified));
					}
				} catch (IOException e) {
					log.error("caught exception", e);
				}
				if (retVal) {
					if (log.isInfoEnabled()) {
						//log.info("modified files : " + modifiedResources);
					}
				}
				return retVal;
			}
		};
		timer = new Timer(true);
		try{
			resetInterval();
		}catch(Exception e){
			
		}
	}



	private Object getParentObject() throws Exception {
		r.lock();
		try {
			return super.getObject();
		} finally {
			r.unlock();
		}
	}

	public SqlSessionFactory getObject() {
		try {
			return this.proxy ==null ? super.getObject() : this.proxy;
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		return null;
	}
	

	public Class<? extends SqlSessionFactory> getObjectType() {
		return (this.proxy != null ? this.proxy.getClass(): SqlSessionFactory.class);
	}

	public boolean isSingleton() {
		return false;
	}

	public void setCheckInterval(int ms) {
		interval = ms;
		if (timer != null) {
			resetInterval();
		}
	}

	private void resetInterval() {
		if (running) {
			timer.cancel();
			running = false;
		}
		if (interval > 0 )  {
			timer.schedule(task, 0, interval);
			running = true;
		}
	}

	public void destroy() throws Exception {
		timer.cancel();
	}
}
