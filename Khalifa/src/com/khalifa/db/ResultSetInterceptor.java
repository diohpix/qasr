package com.khalifa.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.executor.resultset.ResultSetHandler;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;

@Intercepts({ @Signature(type = ResultSetHandler.class, method = "handleResultSets", args = { Statement.class }) })
public class ResultSetInterceptor implements Interceptor {

	@SuppressWarnings("finally")
	@Override
	public Object intercept(Invocation invocation) throws Throwable  {
		List<Object> actual = new ArrayList<Object>();
		Statement statement =null;
		ResultSet rs =null;
		try {
			Object[] args = invocation.getArgs();
			statement = (Statement) args[0];
			rs = statement.getResultSet();
			ResultSetMetaData rsmd = null;
			int columnCount = 0;
			if (rs != null) {
				rsmd = rs.getMetaData();
				columnCount = rsmd.getColumnCount();
				Object[] metaData = new Object[columnCount];
				for (int i = 1; i <= columnCount; i++) {
					Map<String, String> columnMeta = new LinkedHashMap();
					String columnName = rsmd.getColumnName(i);
					String columnLabel = rsmd.getColumnLabel(i);
					if("".equals(columnName)){
						columnName="column"+i;
						columnLabel = columnName;
					}
					String javatype = rsmd.getColumnClassName(i);
					columnMeta.put("columnName", columnName);
					columnMeta.put("columnLabel", columnLabel);
					columnMeta.put("columnType", javatype);
					metaData[i - 1] = columnMeta;
				}
				actual.add(metaData);
			}
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> col = new HashMap<String, Object>();
				for (int i = 1; i <= columnCount; i++) {
					col.put(rsmd.getColumnLabel(i), rs.getObject(i));
				}
				list.add(col);
			}
			actual.add(list);
		} catch (Exception e) {
			throw e;
		} finally{
			if(rs !=null){
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			/*System.out.println(statement);
			if(statement !=null){
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}*/
			return actual;
		}
	}

	@Override
	public Object plugin(Object target) {
		return Plugin.wrap(target, this);
	}

	@Override
	public void setProperties(Properties properties) {
		// To change body of implemented methods use File | Settings | File
		// Templates.
	}

}