package com.qasr.db;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

public class LoadBalancer {
	private List<ReloadableSqlSesseionFactoryBean> connList;
	private int seq;
	private int len;
	public LoadBalancer(){
		this.seq=0;
		this.len=0;
		connList = new ArrayList<>();
	}
	public void addDataSource( ReloadableSqlSesseionFactoryBean sqlSession) {
		this.connList.add(sqlSession);
		this.len++;
	}
	public synchronized SqlSessionFactory getSession(){
		SqlSessionFactory sf=null;
		int c = seq;
		while(true){
			if(seq == len){
				seq=0;
			}
			if(seq==c) new RuntimeException(new Date()+" All  Repliations  Down");
			ReloadableSqlSesseionFactoryBean s= this.connList.get(seq++);
			sf  = s.getObject();
			try{
				SqlSession ss = sf.openSession();
				ss.getConnection().setReadOnly(true);
				ss.close();
				break;
			}catch(Exception e){
				e.printStackTrace();
			/*	this.connList.remove(sf);
				len = this.connList.size();
				seq=0;
				c=-1;*/
			}
		}
		return  sf;
	}
}
