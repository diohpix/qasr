package com.khalifa.transaction;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import com.khalifa.ProtobufUtil;
import com.khalifa.protocol.QueryProtocol.Response;

public class CallableStatement extends Statement {
	private Map<String, Object> output;
	private Map<String, String> byKey;
	private Map<Integer, String> byNum;
	private ResultObject result;
	private int currentResult=0;
	public CallableStatement(TransactionObject tx, String command,int statmentType) {
		super(tx, command,10);
		// TODO Auto-generated constructor stub
	}
	public CallableStatement(TransactionObject tx, String command) {
		super(tx, command,10);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public ResultObject executeQuery() throws IOException,SQLException{
		ResultObject r = super.executeQuery();
		Response res =super.getOutputParam();
		if(res!=null){
			Map<String, String> getterKeymap =new HashMap<String, String>();
			Map<Integer, String> getterNummap =new HashMap<Integer,String>();
			try {
				List<Map<String,Object>> _list = ProtobufUtil.parse(currentResult,res,getterKeymap,getterNummap);
				if(_list!=null && _list.size()>0){
					result = new ResultObject();
					result.setList(_list);
					if(result.next()){
					
					}
				}
			} catch (InvalidProtocolBufferException e) {
				
			}
			this.setKey(getterKeymap, getterNummap);
		}
		return r;
	}
	public void setKey(Map<String, String> k, Map<Integer, String> n) {
		this.byKey = k;
		this.byNum = n;
	}

	public Map<String,Object> getMap(){
		return result.currentRow();
	}
	public Object getObject(String key) {
		return internalObjectByKey(key);
	}

	public String getString(String key) {
		Object a = getObject(key);
		if (a == null)
			return null;
		return a.toString();
	}

	public int getInt(String key) {
		Object a = internalObjectByKey(key);
		if (a == null)
			return 0;
		return ((Number) a).intValue();
	}

	public long getLong(String key) {
		Object a = internalObjectByKey(key);
		if (a == null)
			return 0;
		return ((Number) a).longValue();
	}

	public double getDouble(String key) {
		Object a = internalObjectByKey(key);
		if (a == null)
			return 0;
		return ((Number) a).doubleValue();
	}

	public float getFloat(String key) {
		Object a = internalObjectByKey(key);
		if (a == null)
			return 0;
		return ((Number) a).floatValue();
	}

	public short getShort(String key) {
		Object a = internalObjectByKey(key);
		if (a == null)
			return 0;
		return ((Number) a).shortValue();
	}

	public Time getTime(String key) {
		Object a = internalObjectByKey(key);
		if (a == null)
			return null;
		return (Time) a;
	}

	public Timestamp getTimestamp(String key) {
		Object a = internalObjectByKey(key);
		if (a == null)
			return null;
		return (Timestamp) a;
	}

	public Date getDate(String key) {
		Object a = internalObjectByKey(key);
		if (a == null)
			return null;
		return (Date) a;
	}
	private Object internalObjectByKey(String key) {
		String _k = byKey.get(key.toLowerCase());
		if (_k != null)
			return output.get(_k);
		else
			return output.get(key);
	}

	private Object internalObjectByNum(int num) {
		String _k = byNum.get(num);
		return output.get(_k);
	}

	public Object getObject(int key) {
		return internalObjectByNum(key);
	}

	public String getString(int key) {
		Object a = getObject(key);
		if (a == null)
			return null;
		return a.toString();
	}

	public int getInt(int key) {
		Object a = internalObjectByNum(key);
		if (a == null)
			return 0;
		return ((Number) a).intValue();
	}

	public long getLong(int key) {
		Object a = internalObjectByNum(key);
		if (a == null)
			return 0;
		return ((Number) a).longValue();
	}

	public double getDouble(int key) {
		Object a = internalObjectByNum(key);
		if (a == null)
			return 0;
		return ((Number) a).doubleValue();
	}

	public float getFloat(int key) {
		Object a = internalObjectByNum(key);
		if (a == null)
			return 0;
		return ((Number) a).floatValue();
	}

	public short getShort(int key) {
		Object a = internalObjectByNum(key);
		if (a == null)
			return 0;
		return ((Number) a).shortValue();
	}

	public Time getTime(int key) {
		Object a = internalObjectByNum(key);
		if (a == null)
			return null;
		return (Time) a;
	}

	public Timestamp getTimestamp(int key) {
		Object a = internalObjectByNum(key);
		if (a == null)
			return null;
		return (Timestamp) a;
	}

	public Date getDate(int key) {
		Object a = internalObjectByNum(key);
		if (a == null)
			return null;
		return (Date) a;
	}
	public void close(){
		if(output!=null){
			output.clear();
		}
	}
}
