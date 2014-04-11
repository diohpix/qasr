package com.khalifa.util;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import com.khalifa.protocol.QueryProtocol.DataType;
import com.khalifa.protocol.QueryProtocol.Response;

public class ResponseUtil {
	private static final Logger logger = LoggerFactory.getLogger(ResponseUtil.class);
	private static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss ZZ").serializeNulls().create();

	
	public static String getJSONString(List<Map<String,Object>> list){
		return gson.toJson(list,List.class);
	}
	public static String getJSONString(Map<String,Object> map){
		return gson.toJson(map,Map.class);
	}
	public static Map<String,Object> convertJSONString2Map(String str){
		Map<String,Object> data = null;
		try{
			data = gson.fromJson(str, Map.class);
		}catch(Exception e){
			
		}
		return data;
	}
	/**
	 * 맵을 쿼리 스트링을변환 
	 * @param map
	 * @return
	 */
	public static String convertMap2JSONString(Map<String,Object> map){
		return gson.toJson(map, Map.class);
	}

	public static void makeResponse(ChannelHandlerContext ctx,int code,String msg){
		Response.Builder res = Response.newBuilder();
		Response.Data.Builder data = Response.Data.newBuilder();
		res.setCode(200);
		data.addHeader("msg");
		data.addType(DataType.STRING);
		data.addData(ByteString.copyFromUtf8(msg));
		res.addData(data);
		ctx.writeAndFlush(res.build());
		res.clear();
		logger.debug(res.toString());
	}
	public static void addList2Response(Response.Builder res,List<List<Object>> list){
		for (List<Object> object : list) {
			Response.Data.Builder data = UK.convertObject2Response(object);
			res.addData(data);
		}
	}
}
