package com.khalifa.transaction;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ResultObject {
	private List<Map<String, Object>> list;
	private Map<String, String> byKey;
	private Map<Integer, String> byNum;
	private int code;
	private String error;
	private int length = -1;
	private int curr = -1;

	public void setKey(Map<String, String> k, Map<Integer, String> n) {
		this.byKey = k;
		this.byNum = n;
	}

	public boolean next() {
		curr++;
		return code == 200 && curr < length;
	}

	public List<Map<String, Object>> getList() {
		return list;
	}

	public Map<String, Object> currentRow() {
		return list.get(curr);
	}

	public void putObject(String key, Object value) {
		currentRow().put(key, value);
	}

	public void removeObject(String key) {
		String _k = byKey.get(key.toLowerCase());
		if (_k != null){
			byKey.remove(key.toLowerCase());
			currentRow().remove(_k);
		}else{
			currentRow().remove(key);
		}
	}
	public void clear() {
		currentRow().clear();
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

	public void setList(List<Map<String, Object>> list) {
		this.list = list;
		this.length = list.size();
	}

	public void addResult(int pos, Map<String, Object> item) {
		this.list.add(pos, item);
		this.length = list.size();
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public void close() {
		if (list != null) {
			list.clear();
			list = null;
		}
	}

	private Object internalObjectByKey(String key) {
		String _k = byKey.get(key.toLowerCase());
		if (_k != null)
			return currentRow().get(_k);
		else
			return currentRow().get(key);
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
}
