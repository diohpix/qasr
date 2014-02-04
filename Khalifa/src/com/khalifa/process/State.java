package com.khalifa.process;

import org.apache.ibatis.session.SqlSession;

public class State {
	private StringBuilder log;
	private SqlSession session;
	public State(){
		log = new StringBuilder();
	}
	public StringBuilder getLog() {
		return log;
	}
	public void addLog(String log) {
		this.log.append(log);
	}
	public SqlSession getSession() {
		return session;
	}
	public void setSession(SqlSession session) {
		this.session = session;
	}
	

}
