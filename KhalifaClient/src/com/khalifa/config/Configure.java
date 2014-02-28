package com.khalifa.config;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.khalifa.transaction.Proxy;
import com.khalifa.transaction.ProxyInfo;


/**
 * 설정관리 
 * @author xiphoid
 *
 */
public class Configure {
	private final static Logger log = LoggerFactory.getLogger(Configure.class);
	public static String CONFIG_ROOT="./conf/";
	private static  XMLConfiguration config ;
	public static boolean isReal=false;
	public static void load(File fileName) throws ConfigurationException {
		XMLConfiguration.setDefaultListDelimiter('|');
		config = new XMLConfiguration(fileName);
		
        List<HierarchicalConfiguration> dbproxies = config.configurationsAt("dbproxy.server");
        for (HierarchicalConfiguration server : dbproxies) {
        	String hs = server.getString("[@name]");
        	ProxyInfo pinfo = new ProxyInfo();
    		pinfo.setName(hs);
    		List<HierarchicalConfiguration> hosts = server.configurationsAt("host");
    		for (HierarchicalConfiguration host : hosts) {
    			int port = host.getInt("[@port]");
    			String ip = host.getString("[@ip]");
				Proxy p = new Proxy();
				p.setHost(ip);
				p.setPort(port);
				pinfo.addProxy(p);
			}
    		KhalifaInfo.add(pinfo);
		}
        if(log.isDebugEnabled()){
			log.debug("================================================================");
			log.debug("ProxyClient - System Configure");
			Iterator<String> key = config.getKeys();
			while(key.hasNext()){
				String k = key.next();
				log.debug(k+"="+config.getString(k));
			}
			log.debug("================================================================");
		}
		
	}
	public static String getProperty(String key) {
		String value = config.getProperty(key) == null ? ""
				: config.getProperty(key).toString();
		return value;
	}
	public static int getIntProperty(String key) {
		return config.getInt(key);
	}
	public static boolean getBoolProperty(String key){
		
		return config.getBoolean(key);
	}
	public static Object getObject(String key){
		return config.getProperty(key);
	}
	public static List<Object> getList(String key){
		return config.getList(key);
	}
}