package com.qasr.db;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import com.qasr.util.Configure;


public class ReloadableSqlSesseionFactoryBean extends SqlSessionFactoryBean	implements DisposableBean {

	private final Logger log = LoggerFactory.getLogger(ReloadableSqlSesseionFactoryBean.class);
	private SqlSessionFactory proxy;
	private int interval = Configure.getIntProperty("mapper_check_interval");
	private Timer timer;
	private TimerTask task;
	private Resource[] mapperLocations;

	/**

	 * 파일 감시 쓰레드가 실행중인지 여부.

	 */

	private boolean running = false;
	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	private final Lock r = rwl.readLock();
	private final Lock w = rwl.writeLock();

	public void setMapperLocations(Resource[] mapperLocations) {
		super.setMapperLocations(mapperLocations);
		this.mapperLocations = mapperLocations;
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
				Resource r = mapperLocations[0];
				File f = r.getFile();
				File list[] = f.getParentFile().listFiles();
				mapperLocations = null;
				int l=0;
				for (File file : list) {
					if(file.isFile() && file.getName().endsWith(".xml")){
						l++;
					}
				}
				this.mapperLocations = new Resource[l];
				int i =0;
				for (File file : list) {
					if(file.isFile()&& file.getName().endsWith(".xml")){
						Resource rs =  new  FileSystemResource(file);
						if(rs!=null){
							this.mapperLocations[i++] =rs;
						}
					}
				}
				super.setMapperLocations(mapperLocations);	
			super.afterPropertiesSet();
		} finally {
			w.unlock();
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
						// TODO Auto-generated method stub
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
			// TODO Auto-generated catch block
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
