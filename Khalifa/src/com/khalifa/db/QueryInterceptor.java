package com.khalifa.db;

import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Intercepts({
    @Signature(type=StatementHandler.class, method="update", args={Statement.class})
    , @Signature(type=Executor.class, method="query", args={MappedStatement.class, Object.class, RowBounds.class,   ResultHandler.class})
})
public class QueryInterceptor implements Interceptor {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    @Override
    public Object intercept(Invocation invocation) throws Throwable {
    	if(invocation.getArgs().length > 1 ){
	    	Map param  = (Map) invocation.getArgs()[1];
	    	MappedStatement ms = (MappedStatement) invocation.getArgs()[0];
	    	BoundSql boundSql = ms.getBoundSql(invocation.getArgs()[1]);
	    	List<ParameterMapping> paramMapping = boundSql.getParameterMappings();
	    	for (ParameterMapping parameterMapping : paramMapping) {
	    		String orgkey = parameterMapping.getProperty();
	    		if(orgkey.startsWith("@")){
	    			String nkey = orgkey.substring(1);
	    			Object value = param.get(nkey);
	    			if(value!=null){
	    				param.put(orgkey, value);
	    			}
	    		}
			}
    	}
        return invocation.proceed();
    }

    @Override
    public Object plugin(Object target) {
	// TODO Auto-generated method stub
	return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
	// TODO Auto-generated method stub
    		
    }

}
