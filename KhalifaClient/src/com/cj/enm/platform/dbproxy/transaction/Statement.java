package com.cj.enm.platform.dbproxy.transaction;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import com.cj.enm.platform.dbproxy.exception.InvalidCommandSuffix;
import com.cj.enm.platform.dbproxy.protocol.QueryProtocol.DataType;
import com.cj.enm.platform.dbproxy.protocol.QueryProtocol.Query;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.google.protobuf.ByteString;

public class Statement {
	private static byte [] zlen = new byte[]{-1};
	private Query.Builder res =null;
	private int expireTime;
	private long __insert__id__=-1;
	private TransactionObject tx = null;
	private int sqlType = 0;
	public static boolean DEBUG=false;
	public String toString(){
		List<String> param = res.getParamList();
		int pcount = res.getParamCount();
		List<ByteString> value = res.getValueList();
		StringBuilder sb = new StringBuilder();
		for(int i =0 ; i< pcount;i++){
			sb.append(res.getParam(i)+" "+res.getValue(i).toStringUtf8()+"\n");
		}
		return sb.toString();
	}
	
	public int getExpireTime() {
		return expireTime;
	}
	public void setExpireTime(int expireTime) {
		this.expireTime = expireTime;
		res.setExpire(expireTime);
	}
	public Statement(TransactionObject tx,String command) {
		this.tx = tx;
		  res = Query.newBuilder();
		  try {
			res.setCommand(ByteString.copyFrom(command,"UTF-8"));
			res.setQueryType(getSQLType(command));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	private int getSQLType(String command) {
		if (command.endsWith("SELECT")) {
			sqlType = 1;
		} else if (command.endsWith("INSERT")) {
			sqlType = 2;
		} else if (command.endsWith("DELETE")) {
			sqlType = 3;
		} else if (command.endsWith("UPDATE")) {
			sqlType = 4;
		}
		return sqlType;
	}
	public void clearParameter(){
		ByteString command = res.getCommand();
		int queryType = res.getQueryType();
		res.clear();
		res.setCommand(command);
		res.setQueryType(queryType);
		expireTime=0;
	}
	public void close(){
		res.clear();
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
	/*
	public void setParameter(String key,Object value){
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
	*/
	public ResultObject executeQuery() throws InvalidCommandSuffix, IOException,SQLException{
		if(DEBUG){
			System.out.println(this.toString());
			return null;
		}
		return tx.executeQuery( res);
	}
	public long getLastInsertId(){
		return __insert__id__;
	}
	public int executeUpdate() throws InvalidCommandSuffix, IOException,SQLException{
		if(DEBUG){
			System.out.println(this.toString());
			return -1;
		}
		ResultObject r = executeQuery();
		if(sqlType==2){
				__insert__id__ = ((Long)(r.getList().get(0).get("__insert__id__"))).longValue();
		}
		return ((Integer)(r.getList().get(0).get("__count__"))).intValue();
	}
}