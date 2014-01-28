package com.cj.enm.platform.dbproxy.db.proxy;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

public class ProxySqlSessionFactoryBuilder extends SqlSessionFactoryBuilder {
	@Override
	public SqlSessionFactory build(Configuration config) {
	    return new ProxySqlSessionFactory(config);
	  }
}
