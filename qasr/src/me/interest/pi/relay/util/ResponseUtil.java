package me.interest.pi.relay.util;

import java.util.List;
import java.util.Map;

import me.interest.pi.relay.protocol.QueryProtocol.DataType;
import me.interest.pi.relay.protocol.QueryProtocol.Response;

import org.jboss.netty.channel.ChannelEvent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;

public class ResponseUtil {
	private static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss ZZ").serializeNulls().create();

	
	public static String getJSONString(List<Map<String,String>> list){
		return gson.toJson(list,List.class);
	}
	public static void makeResponse(ChannelEvent event,int code,String msg){
		Response.Builder res = Response.newBuilder();
		res.setCode(200);
		res.addHeader("msg");
		res.addType(DataType.STRING);
		res.addData(ByteString.copyFromUtf8(msg));
		event.getChannel().write(res.build());
	}
}
