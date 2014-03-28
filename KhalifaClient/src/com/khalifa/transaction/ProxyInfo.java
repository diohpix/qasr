package com.khalifa.transaction;

import java.util.ArrayList;
import java.util.List;

public class ProxyInfo {
	private String name;
	private List<Proxy> list = null;
	private int idx=0;
	private int len=0;
	public ProxyInfo(String name){
		list = new ArrayList<Proxy>();
		this.name = name;
	}
	public void addProxy(Proxy proxy){
		proxy.setLive(true);
		list.add(proxy);
		len++;
	}
	public synchronized Proxy getProxy(){
		Proxy p = list.get(idx);
		if(idx+1 == len ){
			idx=0;
		}else{
			idx++;
		}
		return p;
	}
	public String getName() {
		return name;
	}
	public boolean hasAvailServer(){
		boolean has=false;
		for (Proxy p : list) {
			if(p.isLive()){
				has=true;
				break;
			}
		}
		return has;
	}
	public void reset(){
		for (Proxy p : list) {
			p.setLive(true);
		}
	}
}
