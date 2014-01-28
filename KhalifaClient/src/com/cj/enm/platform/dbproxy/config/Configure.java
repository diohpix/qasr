package com.cj.enm.platform.dbproxy.config;

import java.io.File;
import java.util.Iterator;
import java.util.List;


import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cj.enm.platform.dbproxy.transaction.Proxy;
import com.cj.enm.platform.dbproxy.transaction.ProxyInfo;


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
		
        int i=0;
        while(true){
        	String hs = Configure.getProperty("dbproxy.server("+i+")[@name]");
        	if("".equals(hs)) {
        		break;
        	}else{
        		ProxyInfo pinfo = new ProxyInfo();
        		pinfo.setName(hs);
        		int k=0;
        		while(true){
        			String h = Configure.getProperty("dbproxy.server("+i+").host("+k+")[@ip]");
        			if("".equals(h)){
        				break;
        			}else{
        				int port = Configure.getIntProperty("dbproxy.server("+i+").host("+k+")[@port]");
        				System.out.println(hs+" - "+ h+":"+port);
        				Proxy p = new Proxy();
        				p.setHost(h);
        				p.setPort(port);
        				pinfo.addProxy(p);
        			}
        			k++;
        		}
        		DBProxyInfo.add(pinfo);
        	}
        	i++;
        }
        if(log.isDebugEnabled()){
			System.out.println("================================================================");
			System.out.println("ProxyClient - System Configure");
			Iterator<String> key = config.getKeys();
			while(key.hasNext()){
				String k = key.next();
				System.out.println(k+"="+config.getString(k));
			}
			System.out.println("================================================================");
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