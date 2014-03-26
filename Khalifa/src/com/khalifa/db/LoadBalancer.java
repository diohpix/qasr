package com.khalifa.db;

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
	public List<ReloadableSqlSesseionFactoryBean> getSessionList(){
		return this.connList;
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
			SqlSession ss = null;
			try{
				ss = sf.openSession();
				ss.getConnection().setReadOnly(true);
				break;
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				if(ss!=null){
					ss.close();
				}
			}
		}
		return  sf;
	}
}
