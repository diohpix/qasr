package com.qasr.monitor.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.qasr.util.CommonObject;
import com.qasr.util.ResponseUtil;

public class QueryGuide implements ActionInterface{
	private static SqlSessionFactory f = CommonObject.context.getBean("mainSessionFactory", SqlSessionFactory.class);
	public String action(){
		String rtn=null;
		SqlSession sess = null;
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Collection<MappedStatement> s =f.getConfiguration().getMappedStatements();
			List<String> files = new ArrayList<String>();
			for (MappedStatement mappedStatement : s) {
				if(!files.contains(mappedStatement.getResource())){
					files.add(mappedStatement.getResource());
				}
			}
			List<Map<String,String>> list = new ArrayList<Map<String,String>>();
			for (String path : files) {
				String p = path.substring(path.indexOf("[")+1,path.lastIndexOf("]"));
				Document doc = builder.parse(p);
				NodeList newslist = doc.getElementsByTagName("mapper");
				for (int loop = 0; loop < newslist.getLength(); loop++) {
					Node node = newslist.item(loop);
					String ns = node.getAttributes().getNamedItem("namespace").getNodeValue().trim();
					NodeList q = node.getChildNodes();
					for(int i=0;i<q.getLength();i++){
						Map<String,String> info = new HashMap<String,String>();
						Node sql = q.item(i);
						Node nid = sql.getAttributes() !=null ? sql.getAttributes().getNamedItem("id") : null;
						if(nid!=null){
							String id = ns+"."+sql.getAttributes().getNamedItem("id").getNodeValue();
							info.put("sqlType", sql.getNodeName());
							info.put("command",id);
							NodeList item  = sql.getChildNodes();
							info.put("sql",sql.getTextContent().trim().replaceAll("[\\t\\n]", ""));
							for(int j = 0 ; j<item.getLength();j++){
								Node m = item.item(j);
								if(m.getNodeType()==8){
									String c = m.getNodeValue().trim();
									int pp = c.indexOf(":");
									String cmd = c.substring(0,pp);
									switch(cmd){
										case "Q":
											info.put("comment",c.substring(pp+1));
											break;
										case "D":
											info.put("desc",c.substring(pp+1));
											break;
										case "M":
											info.put("method",c.substring(pp+1));
									}
								}
							}
							list.add(info);
						}
					}
				}
			}
			rtn = ResponseUtil.getJSONString(list);
		} catch (Exception e) {

		} finally {
			if (sess != null)
				sess.close();
		}
		return rtn;
	
	}
}
