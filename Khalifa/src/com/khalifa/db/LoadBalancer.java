package com.khalifa.db;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.jdbc.CannotGetJdbcConnectionException;

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
	public synchronized SqlSessionFactory getSession() throws ConnectException{
		SqlSessionFactory sf=null;
		int c = seq;
		int retry=0;
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
				if(e instanceof CannotGetJdbcConnectionException){
					break;
				}
				retry++;
			}finally{
				if(ss!=null){
					ss.close();
				}
				if(retry==10){
					throw new ConnectException("DataBase is down!");
				}
			}
		}
		return  sf;
	}
}
