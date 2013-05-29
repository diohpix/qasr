package com.qasr.protocol.transaction;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.qasr.protocol.APIClientHandler;
import com.qasr.protocol.ProtobufUtil;
import com.qasr.protocol.QueryProtocol.Query;
import com.qasr.protocol.QueryProtocol.Response;
import com.qasr.protocol.exception.InvalidCommandSuffix;

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
	private ClientBootstrap boot;

	public TransactionObject(ClientBootstrap boot,ProxyInfo pinfo ) throws IOException  {
		this.dbname = pinfo.getName();
		this.pinfo = pinfo;
		this.boot = boot;
		connect();
	}
	
	private void connect(){
		int retry=0;
		while(true){
			Proxy px = pinfo.getProxy();
			ChannelFuture connectFuture = boot.connect(new InetSocketAddress(px.getHost(),px.getPort()));
			try {
				this.channel = connectFuture.await().getChannel();
			} catch (InterruptedException e1) {
				this.channel.close();
				e1.printStackTrace();
			}
			this.handler = channel.getPipeline().get(APIClientHandler.class);
			if(this.channel.isConnected()){
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
		return new Statement(this, command);
	}
	
	void setDistributed(boolean v){
		this.isDistributed = v;
	}
	ResultObject executeQuery(Query.Builder query)	throws InvalidCommandSuffix, IOException, SQLException {
		if(!open || !channel.isConnected()) throw new IOException("closed Session");
		query.setDbname(dbname);
		int sqlType = query.getQueryType();
		if (sqlType == 0){
			throw new InvalidCommandSuffix("invalid command ! command must end with _SELECT , _UPDATE , _DELETE , _INSERT");
		}
		if(dbname.indexOf("read")>-1 && (sqlType == 2 || sqlType ==3 || sqlType ==4) ){
			throw new InvalidCommandSuffix("ReadOnly  DB");
		}
		ResultObject result = new ResultObject();
		//Object response = handler.getData(query);
		Object response = handler.getDataDirect(query);
		if (ProtobufUtil.success(response)) { // dbproxy 가 전송한 데이터가 protobuf객체일경우 성공임
			Response res = (Response) response;
			if(res.getCode()==200){
				List<Map<String, Object>> list =null;
				Map<String, String> getterKeymap =new HashMap<String, String>();
				Map<Integer, String> getterNummap =new HashMap<Integer,String>();
				try {
					list = ProtobufUtil.parse(res,getterKeymap,getterNummap);
					result.setList(list);
					result.setCode(res.getCode());
				} catch (InvalidProtocolBufferException e) {
					result.setError(e.getCause()!=null ? e.getCause().getMessage() : e.getMessage());
					result.setCode(500);
				}
				result.setKey(getterKeymap, getterNummap);
			}else{
				result.setCode(res.getCode());
				result.setError(res.getData(0).toStringUtf8());
				throw new SQLException(result.getError());
			}
		} else { // dbproxy 가 전송한 데이터가 string 등의 객체일경우
			result.setError((String) response);
			result.setCode(500);
		}
		return result;
	}
	
	private boolean command(String command) throws IOException{
		if(!open || !channel.isConnected()) throw new IOException("closed Session");
		boolean rtn = false;
		Query.Builder query = Query.newBuilder();
		query.setDbname(dbname);
		query.setQueryType(5);
		query.setCommand(ByteString.copyFrom(command,"UTF-8"));
		//Object response = handler.getData(query);
		Object response = handler.getDataDirect(query);
		if (ProtobufUtil.success(response)) { 
			Response res = (Response) response;
			if(res.getCode()==200){
				List<Map<String, Object>> list =null;
				try {
					list = ProtobufUtil.parse(res,null,null);
					System.out.println(list.get(0).get("msg"));
					rtn = true;
				} catch (InvalidProtocolBufferException e) {
			
				}
				
			}else{
			}
		} 
		return rtn;
	}
	
	public boolean startTransaction() throws IOException	{
		isTransaction = command("BEGIN_TRANSACTION");
		return isTransaction;
	}
	public boolean endTransaction() throws IOException	{
		isTransaction = command("END_TRANSACTION") == false ;
		return isTransaction;
	}
	public boolean commit() throws IOException	{
		if(!open || !channel.isConnected()) throw new IOException("closed Session");
		if(isDistributed) {
			return this.commited=true;
		}
		return command("COMMIT");
	}
	public boolean rollback() throws IOException	{
		if(!open || !channel.isConnected()) throw new IOException("closed Session");
		if(isDistributed) {
			return this.rollbacked=true;
		}
		return command("ROLLBACK");
	}
	public void close()  throws IOException{
	//	if(!open || !channel.isConnected()) throw new IOException("closed Session");
		open = false;
		if(this.channel.isConnected() ){
			ChannelFuture  fu = this.channel.close();
			fu.awaitUninterruptibly(1);
		}
	}
}