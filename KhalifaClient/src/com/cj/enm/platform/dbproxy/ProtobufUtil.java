package com.cj.enm.platform.dbproxy;

import java.io.UnsupportedEncodingException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.cj.enm.platform.dbproxy.protocol.QueryProtocol.DataType;
import com.cj.enm.platform.dbproxy.protocol.QueryProtocol.Response;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class ProtobufUtil {
    	/**
    	 * 응답이 protobuf 인지  확인한다 
    	 * @param objres
    	 * @return
    	 */
    	public static boolean success(Object objres){
    		return objres instanceof Response;
    	}
    	
    	/**
    	 * 성공한 결과를 클라이언트에게 응답 
    	 * @param req
    	 * @param code
    	 * @param list
    	 */
		
		/**
		 * protobuf 객체를 java 객체로 변환 
		 * @param buf
		 * @return
		 * @throws InvalidProtocolBufferException
		 */
		public static List<Map<String, Object>> parse(Response res,Map<String,String> getterKeyList, Map<Integer,String> getterNumList) throws InvalidProtocolBufferException{
			List<String> header = res.getHeaderList();
	    	List<ByteString> datas = res.getDataList();
	    	List<Map<String,Object>> rtn = new ArrayList<Map<String,Object>>();
	    	int loop = datas.size() / header.size();
	    	int idx=0;
	    	for(int i =0 ; i < loop;i++){
	    		Map<String,Object> v = new HashMap<String, Object>();
	    		int tidx=0;
	    		for (String key : header) {
	    			if(i==0){
	    				if(getterKeyList!=null){
	    					getterKeyList.put(key.toLowerCase(), key);
	    				}
	    				if(getterNumList!=null){
	    					getterNumList.put(tidx+1, key);
	    				}
	    			}
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
						Time time = new Time(Longs.fromByteArray(datas.get(idx).toByteArray()));
						v.put(key, time);
						break;
					case TIMESTAMP:
						Timestamp timestamp = new Timestamp(Longs.fromByteArray(datas.get(idx).toByteArray()));
						v.put(key, timestamp);
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
}
