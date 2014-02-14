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
	private List<Map<String, Object>> list;
	private Map<String, String> byKey;
	private Map<Integer, String> byNum;
	private Response res;
	private int curr = -1;

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
		res =super.getOutputParam();
		if(res!=null){
			Map<String, String> getterKeymap =new HashMap<String, String>();
			Map<Integer, String> getterNummap =new HashMap<Integer,String>();
			try {
				list = ProtobufUtil.parse(res,getterKeymap,getterNummap);
			} catch (InvalidProtocolBufferException e) {
				
			}
			curr=0;
			this.setKey(getterKeymap, getterNummap);
		}
		return r;
	}

	public void setKey(Map<String, String> k, Map<Integer, String> n) {
		this.byKey = k;
		this.byNum = n;
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
			return currentRow().get(_k);
		else
			return currentRow().get(key);
	}
	public Map<String, Object> currentRow() {
		return list.get(curr);
	}

	private Object internalObjectByNum(int num) {
		String _k = byNum.get(num);
		return currentRow().get(_k);
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
		if(list!=null){
			list.clear();
		}
		if(res !=null){
			res.getTypeList().clear();
			res.getHeaderList().clear();
			res.getDataList().clear();
		}
	}
}
