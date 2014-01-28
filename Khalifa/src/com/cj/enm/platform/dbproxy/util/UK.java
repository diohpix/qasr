package com.cj.enm.platform.dbproxy.util;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


import com.cj.enm.platform.dbproxy.protocol.QueryProtocol.DataType;
import com.cj.enm.platform.dbproxy.protocol.QueryProtocol.Query;
import com.cj.enm.platform.dbproxy.protocol.QueryProtocol.Response;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public class UK {
	private static byte [] zlen = new byte[]{-1};
	public static byte[] compress(byte[] data) throws IOException {  
		   Deflater deflater = new Deflater();  
		   deflater.setInput(data);  
		   
		   ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);   
		       
		   deflater.finish();  
		   byte[] buffer = new byte[1024];   
		   while (!deflater.finished()) {  
		    int count = deflater.deflate(buffer); // returns the generated code... index  
		    outputStream.write(buffer, 0, count);   
		   }  
		   outputStream.close();  
		   byte[] output = outputStream.toByteArray();  
		   return output;  
		  }  
		   
		  public static byte[] decompress(byte[] data) throws IOException, DataFormatException {  
		   Inflater inflater = new Inflater();   
		   inflater.setInput(data);  
		   
		   ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);  
		   byte[] buffer = new byte[1024];  
		   while (!inflater.finished()) {  
		    int count = inflater.inflate(buffer);  
		    outputStream.write(buffer, 0, count);  
		   }  
		   outputStream.close();  
		   byte[] output = outputStream.toByteArray();  
		   
		   return output;  
		  }  
	
	public static String getWhereString(Map<String,Object> q){
		if(q==null) return null;
		StringBuilder sb = new StringBuilder();
		for(Entry<String, Object> m : q.entrySet()) {
			if(!"#_CK_#".equals(m.getKey())){
				sb.append(",").append(m.getValue());	
			}
		}
		return sb.toString();
	}
	public static  Map<String,Object> getWhere(Query q){
		Map<String,Object> where= null;
		if(q.getParamCount()>0){
			List<String> keys = q.getParamList();
			List<ByteString> value = q.getValueList();
			where= new HashMap<String,Object>();
			StringBuilder sb = null;
			int idx=0;
			for (String param : keys) {
				boolean ckExist = false;
				if(param.startsWith("CK:")){
					param = param.substring(3);
					if(sb==null){
						sb= new StringBuilder();
					}
					ckExist = true;
				}
	        	DataType type =q.getType(idx);
	        	
	        	switch (type) {
				case STRING:
	        		byte [] vv = value.get(idx).toByteArray();
	        		if(vv.length==1 && vv[0] ==-1){
	        			where.put(param,null);
	        		}else{
	        			try {
	        				String ss = new String(vv,"UTF-8");
	        				where.put(param,ss);
	        				if(ckExist){
	    		        		sb.append(",").append(ss);
	    		        	}
						} catch (UnsupportedEncodingException e) {
							e.printStackTrace();
						}
	        		}
					break;
				case SHORT:
					short s = Shorts.fromByteArray(value.get(idx).toByteArray());
	        		where.put(param,s);
	        		if(ckExist){
		        		sb.append(",").append(s);
		        	}
					break;
				case INTEGER:
					int i = Ints.fromByteArray(value.get(idx).toByteArray());
	        		where.put(param,i);
	        		if(ckExist){
		        		sb.append(",").append(i);
		        	}
					break;
				case LONG:
					long l = Longs.fromByteArray(value.get(idx).toByteArray());
	        		where.put(param, l);
	        		if(ckExist){
		        		sb.append(",").append(l);
		        	}
					break;
				case FLOAT:
	        		int intbit = Ints.fromByteArray(value.get(idx).toByteArray());
	        		where.put(param,Float.intBitsToFloat(intbit));
	        		if(ckExist){
		        		sb.append(",").append(intbit);
		        	}
					break;
				case DOUBLE:
	        		long longbit = Longs.fromByteArray(value.get(idx).toByteArray());
	        		where.put(param,Double.longBitsToDouble(longbit));
	        		if(ckExist){
		        		sb.append(",").append(longbit);
		        	}
					break;
				case DATE:
					long l1=Longs.fromByteArray(value.get(idx).toByteArray());
	        		Date date =	new Date(l1);
		        	where.put(param,date);
		        	if(ckExist){
		        		sb.append(",").append(l1);
		        	}
					break;
				case TIME:
					long l2 = Longs.fromByteArray(value.get(idx).toByteArray());
					where.put(param,new Time(l2));
					if(ckExist){
		        		sb.append(",").append(l2);
		        	}
					break;
				case TIMESTAMP:
					long l3= Longs.fromByteArray(value.get(idx).toByteArray());
					where.put(param,new Timestamp(l3));
					if(ckExist){
		        		sb.append(",").append(l3);
		        	}
					break;
				default:
					String str=value.get(idx).toStringUtf8();
	        		where.put(param,str);
	        		if(ckExist){
		        		sb.append(",").append(str);
		        	}
					break;
				}
	        	idx++;
			}
			if(sb!=null){
				where.put("#_CK_#", sb.toString());
			}
		}
		return where;
	}
	@Deprecated
	public static List<Map<String, Object>> convertByte2Object(byte [] buf) throws InvalidProtocolBufferException{ // redis 에서 온 byte 배열 
		Response  res = Response.parseFrom(buf);
		List<String> header = res.getHeaderList();
		List<ByteString> datas = res.getDataList();
		List<Map<String,Object>> rtn = new ArrayList<Map<String,Object>>();
		int loop = datas.size() / header.size();
		int idx=0;
		for(int i =0 ; i < loop;i++){ // JDBC에서 넘어온값 
			Map<String,Object> v = new HashMap<String, Object>();
			int tidx=0;
			for (String key : header) {
	        	DataType type = res.getType(tidx++);
	        	if(datas.get(idx)==ByteString.EMPTY){
	        		v.put(key,null);
	        		idx++;
	        		continue;
	        	}
	        	switch (type) {
				case STRING:
	        		byte [] vv = datas.get(idx).toByteArray();
	        		if(vv.length==1 && vv[0] ==-1){
	        			v.put(key,"");
	        		}else{
	        			try {
							v.put(key,new String(vv,"UTF-8"));
						} catch (UnsupportedEncodingException e) {
							e.printStackTrace();
						}
	        		}
					break;
				case SHORT:
	        		v.put(key,Shorts.fromByteArray(datas.get(idx).toByteArray()));	
					break;
				case INTEGER:
	        		v.put(key,Ints.fromByteArray(datas.get(idx).toByteArray()));
					break;
				case LONG:
	        		v.put(key, Longs.fromByteArray(datas.get(idx).toByteArray()));
					break;
				case FLOAT:
	        		int intbit = Ints.fromByteArray(datas.get(idx).toByteArray());
	        		v.put(key,Float.intBitsToFloat(intbit));
					break;
				case DOUBLE:
	        		long longbit = Longs.fromByteArray(datas.get(idx).toByteArray());
	        		v.put(key,Double.longBitsToDouble(longbit));
					break;
				case DATE:
	        		Date date =	new Date(Longs.fromByteArray(datas.get(idx).toByteArray()));
		        	v.put(key,date);
					break;
				case TIME:
	        		Time time =	new Time(Longs.fromByteArray(datas.get(idx).toByteArray()));
		        	v.put(key,time);
					break;
				case TIMESTAMP:
	        		Timestamp timestamp =	new Timestamp(Longs.fromByteArray(datas.get(idx).toByteArray()));
		        	v.put(key,timestamp);
					break;
				default:
	        		v.put(key,datas.get(idx).toStringUtf8());
					break;
				}
	        	idx++;
			}
	    	rtn.add(v);
	
		}
		return rtn;
	}
	public static Response.Builder convertObject2Response(List<Object> list){
		Response.Builder res = Response.newBuilder();
		if(list.size()>0){ // JDBC 에서 온 값 
			Object[] metaData = (Object[]) list.remove(0);
			for(int i = 0 ; i < metaData.length;i++){
				Map<String,String> k = (Map<String, String>) metaData[i];
				res.addHeader(k.get("columnLabel"));
				String type = k.get("columnType");
				if(type.startsWith("java.lang.String")){
					res.addType(DataType.STRING);
				}else if(type.startsWith("java.lang.Integer")){
					res.addType(DataType.INTEGER);
				}else if(type.startsWith("java.lang.Long") || type.startsWith("java.math.BigDecimal")){
					res.addType(DataType.LONG);
				}else if(type.startsWith("java.lang.Float")){
					res.addType(DataType.FLOAT);
				}else if(type.startsWith("java.lang.Double")){
					res.addType(DataType.DOUBLE);
				}else if(type.startsWith("java.sql.Timestamp")){
					res.addType(DataType.TIMESTAMP);
				}else if(type.startsWith("java.sql.Time")){
					res.addType(DataType.TIME);
				}else if(type.startsWith("java.util.Date")){
					res.addType(DataType.DATE);
				}else if(type.startsWith("java.lang.Short")){
					res.addType(DataType.SHORT);
				}else{
					res.addType(DataType.STRING);
				}
			}
		}
		List<String>mapKey = res.getHeaderList();
		List<Map<String,Object>> lists = (List<Map<String,Object>>) list.get(0);
		for (Map<String,Object> data : lists) {
			for (String key : mapKey) {
					Object value = null;
					if(data ==null){
						res.addData(ByteString.EMPTY);
						continue;
					}
					value = data.get(key);
					if(value instanceof String){
						if("".equals(value.toString())){
							res.addData(ByteString.copyFrom(zlen));
						}else{
							res.addData(ByteString.copyFromUtf8(value.toString()));
						}
					}else if(value instanceof Short){
						res.addData(ByteString.copyFrom(Shorts.toByteArray(((Short) value).shortValue())));
					}else if(value instanceof Integer){
						res.addData(ByteString.copyFrom(Ints.toByteArray(((Integer) value).intValue())));
					}else if(value instanceof Long){
						res.addData(ByteString.copyFrom(Longs.toByteArray(((Long) value).longValue())));
					}else if(value instanceof Float){
						res.addData(ByteString.copyFrom(Ints.toByteArray(Float.floatToIntBits(((Float) value).floatValue()))));
					}else if(value instanceof Double){
						res.addData(ByteString.copyFrom(Longs.toByteArray(Double.doubleToLongBits(((Double) value).doubleValue()))));
					}else if(value instanceof Date){
						long time = ((Date)value).getTime();
						res.addData(ByteString.copyFrom(Longs.toByteArray(time)));
					}else if(value instanceof Time){
						long time = ((Time)value).getTime();
						res.addData(ByteString.copyFrom(Longs.toByteArray(time)));
					}else if(value instanceof Timestamp){
						long time = ((Timestamp)value).getTime();
						res.addData(ByteString.copyFrom(Longs.toByteArray(time)));
					}else if(value == null){
						res.addData(ByteString.EMPTY);
					}else if(value instanceof Clob){
						Clob clob= (Clob)value;
						BufferedReader br =null;
						try {
							br = new BufferedReader(clob.getCharacterStream());
							char[] cbuf =new char[8192];
							int c = 0;
							StringBuilder sb = new StringBuilder();
							while ((c = br.read(cbuf)) > 0){
								sb.append(cbuf, 0, c);
							}
							res.addData(ByteString.copyFromUtf8(sb.toString()));
						} catch (Exception e) {
							e.printStackTrace();
						}finally{
							try {
								br.close();
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}else if(value instanceof java.math.BigDecimal){
						BigDecimal b = ((BigDecimal)value).setScale(0);
						res.addData(ByteString.copyFrom(Longs.toByteArray(b.longValue())));
					}else{
						System.out.println(value.getClass().getName());
						res.addData(ByteString.copyFromUtf8( value.toString()));
					}
			}
		}
		return res;
	}
}
