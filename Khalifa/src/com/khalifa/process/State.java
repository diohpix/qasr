package com.khalifa.process;

import org.apache.ibatis.session.SqlSession;

import com.khalifa.protocol.QueryProtocol.Response;

public class State {
	private boolean readTransaction=false;
	private StringBuilder log;
	private SqlSession session;
	private Response.Builder outputParam;
	public Response.Builder getOutputParam() {
		return outputParam;
	}
	public void setOutputParam(Response.Builder outputParam) {
		this.outputParam = outputParam;
	}
	public State(){
		log = new StringBuilder();
	}
	public StringBuilder getLog() {
		return log;
	}
	public boolean isReadTransaction() {
		return readTransaction;
	}
	public void setReadTransaction(boolean readTransaction) {
		this.readTransaction = readTransaction;
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
	public void clear(){
		if(this.session!=null){
			session.close();
			session = null;
		}
		if(this.outputParam !=null){
			outputParam.clear();
			outputParam = null;
		}
		log = null;
	}
}
