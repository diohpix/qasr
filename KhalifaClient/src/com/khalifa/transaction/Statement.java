package com.khalifa.transaction;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.google.protobuf.ByteString;
import com.khalifa.protocol.QueryProtocol.DataType;
import com.khalifa.protocol.QueryProtocol.Query;
import com.khalifa.protocol.QueryProtocol.Response;

public class Statement {
	private static final Logger logger = LoggerFactory.getLogger(Statement.class); 
	private static byte [] zlen = new byte[]{-1};
	private static byte [] TRUE = new byte[]{1};
	private static byte [] FALSE = new byte[]{0};
	
	private Query.Data.Builder res =null;
	private Query.Builder query =null;
	private int expireTime;
	private int statmentType;
	private long __insert__id__=-1;
	private TransactionObject tx = null;
	private long lastID[] = null;
	public String toString(){
		int pcount = res.getParamCount();
		StringBuilder sb = new StringBuilder();
		for(int i =0 ; i< pcount;i++){
			sb.append(res.getParam(i)+" "+res.getValue(i).toStringUtf8()+"\n");
		}
		return sb.toString();
	}
	/**
	 * SELECT 쿼리 expire 타임 
	 * @return
	 */
	public int getExpireTime() {
		return expireTime;
	}
	/**
	 * SELECT 한 결과를 캐시유지 기간 
	 * @param expireTime
	 */
	public void setExpireTime(int expireTime) {
		this.expireTime = expireTime;
		query.setExpire(expireTime);
	}
	/**
	 * ResultObject 가 더 있는지 확인 
	 * @return
	 */
	public boolean getMoreResult(){
		return tx.hasMoreResult();
	}
	/**
	 * 다음 ResultObject 가져옴 
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	public ResultObject getMoreResultObject() throws IOException, SQLException{
		return tx.moreResult();
	}
	Statement(TransactionObject tx,String command) {
		this.tx = tx;
		  query = Query.newBuilder();
		  res = Query.Data.newBuilder();
		  this.statmentType = 0;
		  try {
			query.setCommand(ByteString.copyFrom(command,"UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	Statement(TransactionObject tx,String command,int statmentType) {
		this.tx = tx;
		  query = Query.newBuilder();
		  res = Query.Data.newBuilder();
		  this.statmentType = statmentType;
		  try {
			query.setCommand(ByteString.copyFrom(command,"UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	public void clearParameter(){
		ByteString command = query.getCommand();
		int queryType = query.getQueryType();
		query.clear();
		query.setCommand(command);
		query.setQueryType(queryType);
		expireTime=0;
		res.clear();
	}
	public void close(){
		query.clearData();
		query.clear();
		lastID=null;
	}
	public void setString(String key,String value){
		res.addParam(key);
		if(value ==null || "".equals(value.toString()) ){
			res.addValue(ByteString.copyFrom(zlen));
		}else{
			res.addValue(ByteString.copyFromUtf8(value.toString()));
		}
		res.addType(DataType.STRING);
	}
	public void setShort(String key ,short value){
		res.addParam(key);
		res.addValue(ByteString.copyFrom(Shorts.toByteArray(value)));
		res.addType(DataType.SHORT);
	}
	public void setBoolean(String key ,boolean value){
		res.addParam(key);
		if(value){
			res.addValue(ByteString.copyFrom(TRUE));
		}else{
			res.addValue(ByteString.copyFrom(FALSE));
		}
		res.addType(DataType.BOOLEAN);
	}
	public void setInt(String key,int value){
		res.addParam(key);
		res.addValue(ByteString.copyFrom(Ints.toByteArray(value)));
		res.addType(DataType.INTEGER);
	}
	public void setLong(String key,long value){
		res.addParam(key);
		res.addValue(ByteString.copyFrom(Longs.toByteArray(value)));
		res.addType(DataType.LONG);
	}
	public void setFloat(String key,float value){
		res.addParam(key);
		res.addValue(ByteString.copyFrom(Ints.toByteArray(Float.floatToIntBits(value))));
		res.addType(DataType.FLOAT);
	}
	public void setDouble(String key,double value){
		res.addParam(key);
		res.addValue(ByteString.copyFrom(Longs.toByteArray(Double.doubleToLongBits(value))));
		res.addType(DataType.DOUBLE);
	}
	public void setDate(String key ,Date value){
		long time = value.getTime();
		res.addParam(key);
		res.addValue(ByteString.copyFrom(Longs.toByteArray(time)));
		res.addType(DataType.DATE);
	}
	public void setTime(String key ,Time value){
		// hh:mm:ss
		long time = value.getTime();
		res.addParam(key);
		res.addValue(ByteString.copyFrom(Longs.toByteArray(time)));
		res.addType(DataType.TIME);
	}
	public void setTime(String key ,String value){
		setTime(key, Time.valueOf(value));
	}
	
	public void setTimestamp(String key ,Timestamp value){
		long time = value.getTime();
		res.addParam(key);
		res.addValue(ByteString.copyFrom(Longs.toByteArray(time)));
		res.addType(DataType.TIMESTAMP);
	}
	
	public void setMap(Map<String,Object> map){
		for (Entry<String, Object> entry : map.entrySet())
		{
		    String key = entry.getKey();
		    Object value = entry.getValue();
		    res.addParam(key);
		    if(value instanceof String){
				if("".equals(value.toString())){
					res.addValue(ByteString.copyFrom(zlen));
				}else{
					res.addValue(ByteString.copyFromUtf8(value.toString()));
				}
				res.addType(DataType.STRING);
			}else if(value instanceof Short){
				res.addValue(ByteString.copyFrom(Shorts.toByteArray(((Short) value).shortValue())));
				res.addType(DataType.SHORT);
			}else if(value instanceof Integer){
				res.addValue(ByteString.copyFrom(Ints.toByteArray(((Integer) value).intValue())));
				res.addType(DataType.INTEGER);
			}else if(value instanceof Long){
				res.addValue(ByteString.copyFrom(Longs.toByteArray(((Long) value).longValue())));
				res.addType(DataType.LONG);
			}else if(value instanceof Float){
				res.addValue(ByteString.copyFrom(Ints.toByteArray(Float.floatToIntBits(((Float) value).floatValue()))));
				res.addType(DataType.FLOAT);
			}else if(value instanceof Double){
				res.addValue(ByteString.copyFrom(Longs.toByteArray(Double.doubleToLongBits(((Double) value).doubleValue()))));
				res.addType(DataType.DOUBLE);
			}else if(value instanceof Date){
				long time = ((Date)value).getTime();
				res.addValue(ByteString.copyFrom(Longs.toByteArray(time)));
				res.addType(DataType.DATE);
			}else if(value instanceof Time){
				long time = ((Timestamp) value).getTime();
				res.addValue(ByteString.copyFrom(Longs.toByteArray(time)));
				res.addType(DataType.TIME);
			}else if(value instanceof Timestamp){
				long time = ((Timestamp) value).getTime();
				res.addValue(ByteString.copyFrom(Longs.toByteArray(time)));
				res.addType(DataType.TIMESTAMP);
			}
		}
	}
	
	public void setTimestamp(String key ,String value){
		// yyyy-mm-dd hh:mm:ss 일경우만 
		setTimestamp(key, Timestamp.valueOf(value));
	}
	
	Response getOutputParam() throws IOException{
		return tx.outputParam();
	}
	public ResultObject executeQuery() throws IOException,SQLException{
		if(logger.isDebugEnabled()){
			logger.debug("executeQuery ",this);
		//	return null;
		}
		if(statmentType==10){
			query.setQueryType(10);
		}else{
			query.setQueryType(1);
		}
		if(query.getDataCount()==0){
			query.addData(res);
		}
		return tx.executeQuery(query);
	}
	public long getLastInsertId(){
		return __insert__id__;
	}
	public int executeUpdate() throws IOException,SQLException{
		if(logger.isDebugEnabled()){
			logger.debug("executeUpdate {}",this.toString());
		}
		query.setQueryType(2);
		if(query.getDataCount()==0){
			query.addData(res);
		}
		ResultObject r = executeQuery();
		if(r.getList().get(0).get("__insert__id__")!=null){
				__insert__id__ = ((Long)(r.getList().get(0).get("__insert__id__"))).longValue();
		}
		return ((Integer)(r.getList().get(0).get("__count__"))).intValue();
	}
	public long[] getBatchLastID(){
		return lastID;
	}
	public int[] executeBatch() throws IOException,SQLException{
		if(logger.isDebugEnabled()){
			logger.debug("executeUpdate {}",this.toString());
		}
		query.setQueryType(2);
		ResultObject r = executeQuery();
		int count = r.getList().size();
		int rtn[] = new int[count];
		int rcount = 0;
		while(r.next()){
			int rc  = r.getInt("__count__");
			rtn[rcount]=rc;
			if(r.getString("__insert__id__")!=null){
				if(lastID ==null){
					lastID = new long[count];
				}
				lastID[rcount] = r.getLong("__insert__id__");
			}
			rcount++;
		}
		return rtn;
	}
	public void addBatch(){
		query.addData(res);
		res = Query.Data.newBuilder();
	}
}