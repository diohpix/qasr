package com.khalifa.config;

import java.util.HashMap;
import java.util.Map;

import com.khalifa.transaction.ProxyInfo;


class KhalifaInfo {
	private static Map<String,ProxyInfo> info =new HashMap<String, ProxyInfo>();
	public static void add(ProxyInfo pinfo){
		info.put(pinfo.getName(), pinfo);
	}
	public static synchronized ProxyInfo getInfo(String name){
		ProxyInfo pinfo= info.get(name);
		if(pinfo ==null){
			pinfo = info.get("*");
		}
		return pinfo;
		
	}
}
