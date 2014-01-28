package com.cj.enm.platform.dbproxy.db.proxy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.binding.BindingException;
import org.apache.ibatis.exceptions.ExceptionFactory;
import org.apache.ibatis.exceptions.TooManyResultsException;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.result.DefaultMapResultHandler;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;

public class ProxySqlSession implements SqlSession {

	private Configuration configuration;
	private Executor executor;

	private boolean dirty;

	@Deprecated
	public ProxySqlSession(Configuration configuration, Executor executor,
			boolean autoCommit) {
		this(configuration, executor);
	}

	public ProxySqlSession(Configuration configuration, Executor executor) {
		this.configuration = configuration;
		this.executor = executor;
		this.dirty = false;
	}

	public <T> T selectOne(String statement) {
		return this.<T> selectOne(statement, null);
	}

	public <T> T selectOne(String statement, Object parameter) {
		// Popular vote was to return null on 0 results and throw exception on
		// too many.
		List<T> list = this.<T> selectList(statement, parameter);
		if (list.size() == 1) {
			return list.get(0);
		} else if (list.size() > 1) {
			throw new TooManyResultsException(
					"Expected one result (or null) to be returned by selectOne(), but found: "
							+ list.size());
		} else {
			return null;
		}
	}

	public <K, V> Map<K, V> selectMap(String statement, String mapKey) {
		return this
				.<K, V> selectMap(statement, null, mapKey, RowBounds.DEFAULT);
	}

	public <K, V> Map<K, V> selectMap(String statement, Object parameter,
			String mapKey) {
		return this.<K, V> selectMap(statement, parameter, mapKey,
				RowBounds.DEFAULT);
	}

	public <K, V> Map<K, V> selectMap(String statement, Object parameter,
			String mapKey, RowBounds rowBounds) {
		final List<?> list = selectList(statement, parameter, rowBounds);
		final DefaultMapResultHandler<K, V> mapResultHandler = new DefaultMapResultHandler<K, V>(
				mapKey, configuration.getObjectFactory(),
				configuration.getObjectWrapperFactory());
		final DefaultResultContext context = new DefaultResultContext();
		for (Object o : list) {
			context.nextResultObject(o);
			mapResultHandler.handleResult(context);
		}
		Map<K, V> selectedMap = mapResultHandler.getMappedResults();
		return selectedMap;
	}

	public <E> List<E> selectList(String statement) {
		return this.<E> selectList(statement, null);
	}
	public <E> List<E> selectList(MappedStatement ms) {
		return this.<E> selectList(ms, null);
	}
	public <E> List<E> selectList(String statement, Object parameter) {
		return this.<E> selectList(statement, parameter, RowBounds.DEFAULT);
	}
	public <E> List<E> selectList(MappedStatement ms, Object parameter) {
		return this.<E> selectList(ms, parameter, RowBounds.DEFAULT);
	}
	public <E> List<E> selectList(String statement, Object parameter,
			RowBounds rowBounds) {
		try {
			MappedStatement ms = configuration.getMappedStatement(statement);
			List<E> result = executor.<E> query(ms, wrapCollection(parameter),
					rowBounds, Executor.NO_RESULT_HANDLER);
			return result;
		} catch (Exception e) {
			throw ExceptionFactory.wrapException(
					"Error querying database.  Cause: " + e, e);
		} finally {
			ErrorContext.instance().reset();
		}
	}

	public <E> List<E> selectList(MappedStatement ms, Object parameter,	RowBounds rowBounds) {
		try {
			List<E> result = executor.<E> query(ms, wrapCollection(parameter),
					rowBounds, Executor.NO_RESULT_HANDLER);
			return result;
		} catch (Exception e) {
			throw ExceptionFactory.wrapException(
					"Error querying database.  Cause: " + e, e);
		} finally {
			ErrorContext.instance().reset();
		}
	}

	public void select(String statement, Object parameter, ResultHandler handler) {
		select(statement, parameter, RowBounds.DEFAULT, handler);
	}

	public void select(String statement, ResultHandler handler) {
		select(statement, null, RowBounds.DEFAULT, handler);
	}

	public void select(String statement, Object parameter, RowBounds rowBounds,
			ResultHandler handler) {
		try {
			MappedStatement ms = configuration.getMappedStatement(statement);
			executor.query(ms, wrapCollection(parameter), rowBounds, handler);
		} catch (Exception e) {
			throw ExceptionFactory.wrapException(
					"Error querying database.  Cause: " + e, e);
		} finally {
			ErrorContext.instance().reset();
		}
	}

	public int insert(String statement) {
		return insert(statement, null);
	}
	public int insert(MappedStatement statement) {
		return insert(statement, null);
	}
	public int insert(String statement, Object parameter) {
		return update(statement, parameter);
	}
	public int insert(MappedStatement statement, Object parameter) {
		return update(statement, parameter);
	}
	public int update(String statement) {
		return update(statement, null);
	}
	public int update(MappedStatement statement) {
		return update(statement, null);
	}
	public int update(String statement, Object parameter) {
		try {
			dirty = true;
			MappedStatement ms = configuration.getMappedStatement(statement);
			return executor.update(ms, wrapCollection(parameter));
		} catch (Exception e) {
			throw ExceptionFactory.wrapException(
					"Error updating database.  Cause: " + e, e);
		} finally {
			ErrorContext.instance().reset();
		}
	}
	public int update(MappedStatement ms, Object parameter) {
		try {
			dirty = true;
			return executor.update(ms, wrapCollection(parameter));
		} catch (Exception e) {
			throw ExceptionFactory.wrapException(
					"Error updating database.  Cause: " + e, e);
		} finally {
			ErrorContext.instance().reset();
		}
	}

	public int delete(String statement) {
		return update(statement, null);
	}
	public int delete(MappedStatement statement) {
		return update(statement, null);
	}
	public int delete(String statement, Object parameter) {
		return update(statement, parameter);
	}
	public int delete(MappedStatement statement, Object parameter) {
		return update(statement, parameter);
	}
	public void commit() {
		commit(false);
	}

	public void commit(boolean force) {
		try {
			executor.commit(isCommitOrRollbackRequired(force));
			dirty = false;
		} catch (Exception e) {
			throw ExceptionFactory.wrapException(
					"Error committing transaction.  Cause: " + e, e);
		} finally {
			ErrorContext.instance().reset();
		}
	}

	public void rollback() {
		rollback(false);
	}

	public void rollback(boolean force) {
		try {
			executor.rollback(isCommitOrRollbackRequired(force));
			dirty = false;
		} catch (Exception e) {
			throw ExceptionFactory.wrapException(
					"Error rolling back transaction.  Cause: " + e, e);
		} finally {
			ErrorContext.instance().reset();
		}
	}

	public List<BatchResult> flushStatements() {
		try {
			return executor.flushStatements();
		} catch (Exception e) {
			throw ExceptionFactory.wrapException(
					"Error flushing statements.  Cause: " + e, e);
		} finally {
			ErrorContext.instance().reset();
		}
	}

	public void close() {
		try {
			executor.close(isCommitOrRollbackRequired(false));
			dirty = false;
		} finally {
			ErrorContext.instance().reset();
		}
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public <T> T getMapper(Class<T> type) {
		return configuration.<T> getMapper(type, this);
	}

	public Connection getConnection() {
		try {
			return executor.getTransaction().getConnection();
		} catch (SQLException e) {
			throw ExceptionFactory.wrapException(
					"Error getting a new connection.  Cause: " + e, e);
		}
	}

	public void clearCache() {
		executor.clearLocalCache();
	}

	private boolean isCommitOrRollbackRequired(boolean force) {
		return dirty || force;
	}

	private Object wrapCollection(final Object object) {
		if (object instanceof List) {
			StrictMap<Object> map = new StrictMap<Object>();
			map.put("list", object);
			return map;
		} else if (object != null && object.getClass().isArray()) {
			StrictMap<Object> map = new StrictMap<Object>();
			map.put("array", object);
			return map;
		}
		return object;
	}

	public static class StrictMap<V> extends HashMap<String, V> {

		private static final long serialVersionUID = -5741767162221585340L;

		@Override
		public V get(Object key) {
			if (!super.containsKey(key)) {
				throw new BindingException("Parameter '" + key
						+ "' not found. Available parameters are "
						+ this.keySet());
			}
			return super.get(key);
		}

	}
}
