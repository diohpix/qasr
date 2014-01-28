package com.cj.enm.platform.dbproxy.config;

import java.util.HashMap;
import java.util.Map;

import com.cj.enm.platform.dbproxy.transaction.ProxyInfo;


class DBProxyInfo {
	private static Map<String,ProxyInfo> info =new HashMap<String, ProxyInfo>();
	public static void add(ProxyInfo pinfo){
		info.put(pinfo.getName(), pinfo);
	}
	public static synchronized ProxyInfo getInfo(String name){
		return info.get(name);
		
	}
}
