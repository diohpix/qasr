package com.khalifa.transaction;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.khalifa.APIClientHandler;
import com.khalifa.ProtobufUtil;
import com.khalifa.protocol.QueryProtocol.Query;
import com.khalifa.protocol.QueryProtocol.Response;

public class TransactionObject {
	private Channel channel = null;
	private APIClientHandler handler = null;
	private boolean open=false;
	private String dbname;
	private boolean isDistributed;
	private boolean isTransaction;
	private boolean commited;
	private boolean rollbacked;
	private ProxyInfo pinfo;
	private Bootstrap boot;
	private int currentResult=0;
	private Response res;
	private static final Logger logger = LoggerFactory.getLogger(TransactionObject.class);
	
	public TransactionObject(Bootstrap boot,ProxyInfo pinfo,String aliasName ) throws IOException  {
		this.dbname =aliasName;// pinfo.getName();
		this.pinfo = pinfo;
		this.boot = boot;
		connect();
	}
	public String getName(){
		return this.dbname;
	}
	private void connect(){
		while(true){
			Proxy px = pinfo.getProxy();
			ChannelFuture connectFuture = null;
			try {
				connectFuture = boot.connect(new InetSocketAddress(px.getHost(),px.getPort())).sync();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.channel = connectFuture.channel();
			this.handler = channel.pipeline().get(APIClientHandler.class);
			if(this.channel.isActive()){
				open = true;
				break;
			}else{
				px.setLive(false);
				if(!pinfo.hasAvailServer()){
					pinfo.reset();
					throw new RuntimeException("NO AVAILABLE SERVER");
				}
			}
		}
	}
	public Statement prepareStatement(String command) throws IOException{
		if(!open) throw new IOException("closed Session");
		return new Statement(this, command,0);
	}
	public CallableStatement callableStatement(String command) throws IOException{
		if(!open) throw new IOException("closed Session");
		return new CallableStatement(this, command);
	}
	
	void setDistributed(boolean v){
		this.isDistributed = v;
	}
	boolean hasMoreResult(){
		currentResult++;
		if(currentResult < res.getDataCount() ){
			return true;
		}else{
			return false;
		}
	}
	ResultObject moreResult()	throws  IOException, SQLException {
		ResultObject result = new ResultObject();
		List<Map<String, Object>> list =null;
		Map<String, String> getterKeymap =new LinkedHashMap<String, String>();
		Map<Integer, String> getterNummap =new LinkedHashMap<Integer,String>();
		try {
			list = ProtobufUtil.parse(currentResult,res,getterKeymap,getterNummap);
			result.setList(list);
			result.setCode(res.getCode());
			result.setKey(getterKeymap, getterNummap);
		} catch (InvalidProtocolBufferException e) {
			result.setError(e.getCause()!=null ? e.getCause().getMessage() : e.getMessage());
			result.setCode(500);
		}
		return result;
	}
	ResultObject executeQuery(Query.Builder query)	throws  IOException, SQLException {
		if(!open || !channel.isActive()) throw new IOException("closed Session");
		query.setDbname(dbname);
		int sqlType = query.getQueryType();
		if (sqlType == 0){
			throw new RuntimeException("SQLType Error");
		}
		if(dbname.indexOf("read")>-1 && (sqlType == 2 || sqlType ==3 || sqlType ==4) ){
			throw new RuntimeException("ReadOnly  DB");
		}
		ResultObject result = new ResultObject();
		Object response = handler.getDataDirect(this.channel,query);
		if (ProtobufUtil.success(response)) { // dbproxy 가 전송한 데이터가 protobuf객체일경우 성공임
			res = (Response) response;
			if(res.getCode()==200){
				List<Map<String, Object>> list =null;
				Map<String, String> getterKeymap =new LinkedHashMap<String, String>();
				Map<Integer, String> getterNummap =new LinkedHashMap<Integer,String>();
				try {
					list = ProtobufUtil.parse(currentResult,res,getterKeymap,getterNummap);
					result.setList(list);
					result.setCode(res.getCode());
				} catch (InvalidProtocolBufferException e) {
					result.setError(e.getCause()!=null ? e.getCause().getMessage() : e.getMessage());
					result.setCode(500);
				}
				result.setKey(getterKeymap, getterNummap);
			}else{
				this.open = false;
				int errType = res.getCode();
				result.setCode(errType);
				result.setError(res.getData(currentResult).getData(0).toStringUtf8());
				if(errType==600){
					if(res.getDataCount() ==4){
						SQLException sqle = new SQLException(res.getData(currentResult).getData(1).toStringUtf8(),res.getData(currentResult).getData(2).toStringUtf8(),Integer.valueOf(res.getData(currentResult).getData(3).toStringUtf8()));
						throw sqle;
					}else{
						throw new RuntimeException(result.getError());
					}
				}else{
					throw new RuntimeException(result.getError());
				}
			}
		} else { // dbproxy 가 전송한 데이터가 string 등의 객체일경우
			result.setError((String) response);
			result.setCode(500);
		}
		return result;
	}
	
	private boolean command(String command) throws IOException{
		if(!open || !channel.isActive()) throw new IOException("closed Session");
		boolean rtn = false;
		Query.Builder query = Query.newBuilder();
		query.setDbname(dbname);
		query.setQueryType(5);
		query.setCommand(ByteString.copyFrom(command,"UTF-8"));
		logger.debug("SEND COMMAND {}",command);
		//Object response = handler.getData(query);
		Object response = handler.getDataDirect(this.channel,query);
		if (ProtobufUtil.success(response)) { 
			Response res = (Response) response;
			if(res.getCode()==200){
				List<Map<String, Object>> list =null;
				try {
					list = ProtobufUtil.parse(0,res,null,null);
					rtn = true;
				} catch (InvalidProtocolBufferException e) {
			
				}
				
			}else{
			}
		} 
		return rtn;
	}
	public Response outputParam() throws IOException{
		if(!open || !channel.isActive()) throw new IOException("closed Session");
		Response res = null;
		Query.Builder query = Query.newBuilder();
		query.setDbname(dbname);
		query.setQueryType(5);
		query.setCommand(ByteString.copyFrom("OUTPUTPARAM","UTF-8"));
		Object response = handler.getDataDirect(this.channel,query);
		if (ProtobufUtil.success(response)) { 
			 res = (Response) response;
		} 
		return res;
	}
	
	public boolean startTransaction() throws IOException{
		isTransaction = command("BEGIN_TRANSACTION");
		return isTransaction;
	}
	public boolean startReadOnlyTransaction() throws IOException{
		isTransaction = command("BEGIN_R_TRANSACTION");
		return isTransaction;
	}
	public boolean endTransaction() throws IOException	{
		isTransaction = command("END_TRANSACTION") == false ;
		return isTransaction;
	}
	public boolean commit() throws IOException	{
		if(!open || !channel.isActive()) throw new IOException("closed Session");
		if(isDistributed) {
			return this.commited=true;
		}
		return command("COMMIT");
	}
	public boolean rollback() throws IOException	{
		if(!open || !channel.isActive()) throw new IOException("closed Session");
		if(isDistributed) {
			return this.rollbacked=true;
		}
		return command("ROLLBACK");
	}
	public boolean isTransaction(){
		return this.isTransaction;
	}
	public void close()  throws IOException{
		logger.debug("close "+open);
		if(this.channel.isActive()){
			try {
				this.channel.close().sync();
			} catch (InterruptedException e) {
			}
		}
		open = false;
	}
}