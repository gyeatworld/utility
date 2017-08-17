package cn.com.bestv.utility;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import cn.com.bestv.infrastructure.common.PagerInfo;
import cn.com.bestv.infrastructure.common.QueryResult;
import cn.com.bestv.infrastructure.common.SortDirection;
import cn.com.bestv.infrastructure.common.SortInfo;

/**
 * @author Vem
 * @date 创建时间：2017年8月17日 下午1:28:09
 * @version 1.0
 * @parameter
 * @since
 * @return
 */
public class DruidUtil {
	private static DataSource[] readDataSource;// 读数据源
	private static DataSource[] writeDataSource;// 写数据源

	// private static int CountOfReadDataSource=-1;
	// private static int CountOfWriteDataSource=-1;
	/***
	 * 获取数据源
	 * 
	 * @return
	 * @throws Exception
	 */
	private static DataSource[] getReadDataSources() throws SQLException {
		if (readDataSource != null && readDataSource.length > 0) {
			return readDataSource;
		}
		DataSource[] dataSource;
		try {
			Properties propPortal = new Properties();
			propPortal.load(DruidUtil.class.getClassLoader().getResourceAsStream("app.properties"));

			String strReadDataSourceCount = propPortal.getProperty("db.read_datasource_count");

			int iReadDataSourceCount = Integer.parseInt(strReadDataSourceCount);

			String strReadDataSourceIndex = "00";

			dataSource = new DataSource[iReadDataSourceCount];

			for (int i = 0; i < iReadDataSourceCount; i++) {
				Properties propReadDataSource = new Properties();

				strReadDataSourceIndex = String.format("%02d", i);
				propReadDataSource.load(DruidUtil.class.getClassLoader()
						.getResourceAsStream("druid_read_" + strReadDataSourceIndex + ".properties"));
				Class.forName(propReadDataSource.getProperty("driverClassName"));

				dataSource[i] = DruidDataSourceFactory.createDataSource(propReadDataSource);

				/*
				 * String defaultAutoCommit = propReadDataSource
				 * .getProperty("bonecp.defaultAutoCommit"); String defaultReadOnly =
				 * propReadDataSource .getProperty("bonecp.defaultReadOnly"); BoneCPConfig
				 * config = new BoneCPConfig(propReadDataSource); if
				 * (StringUtils.hasText(defaultAutoCommit)) {
				 * config.setDefaultAutoCommit(Boolean .valueOf(defaultAutoCommit)); } if
				 * (StringUtils.hasText(defaultReadOnly)) {
				 * config.setDefaultReadOnly(Boolean.valueOf(defaultReadOnly)); }
				 */
				// readDataSource[i] = new BoneCPDataSource(config);
			}
			readDataSource = dataSource;
		} catch (Exception e) {
			readDataSource = null;
			throw new SQLException(e);

		}
		return dataSource;
	}

	private static DataSource[] getWriteDataSources() throws SQLException {
		if (writeDataSource != null && writeDataSource.length > 0) {
			return writeDataSource;
		}
		DataSource[] dataSource;
		try {
			Properties propPortal = new Properties();
			propPortal.load(DruidUtil.class.getClassLoader().getResourceAsStream("app.properties"));

			String strWriteDataSourceCount = propPortal.getProperty("db.write_datasource_count");
			int iWriteDataSourceCount = Integer.parseInt(strWriteDataSourceCount);

			String strWriteDataSourceIndex = "00";
			dataSource = new DataSource[iWriteDataSourceCount];

			for (int i = 0; i < iWriteDataSourceCount; i++) {
				Properties propWriteDataSource = new Properties();

				strWriteDataSourceIndex = String.format("%02d", i);
				propWriteDataSource.load(DruidUtil.class.getClassLoader()
						.getResourceAsStream("druid_write_" + strWriteDataSourceIndex + ".properties"));
				Class.forName(propWriteDataSource.getProperty("driverClassName"));

				dataSource[i] = DruidDataSourceFactory.createDataSource(propWriteDataSource);

				/*
				 * String defaultAutoCommit = propWriteReadDataSource
				 * .getProperty("bonecp.defaultAutoCommit"); String defaultReadOnly =
				 * propWriteReadDataSource .getProperty("bonecp.defaultReadOnly"); BoneCPConfig
				 * config = new BoneCPConfig(propWriteReadDataSource); if
				 * (StringUtils.hasText(defaultAutoCommit)) {
				 * config.setDefaultAutoCommit(Boolean .valueOf(defaultAutoCommit)); } if
				 * (StringUtils.hasText(defaultReadOnly)) {
				 * config.setDefaultReadOnly(Boolean.valueOf(defaultReadOnly)); }
				 * writeDataSource[i] = new BoneCPDataSource(config);
				 */

			}
			writeDataSource = dataSource;
		} catch (Exception e) {
			writeDataSource = null;
			throw new SQLException(e);
		}
		return dataSource;
	}

	// 获得Connection读写分离
	public static Connection getRandomReadConnection() throws SQLException {
		int iFeed = (int) (Math.random() * 1000);
		DataSource[] readDataSource = getReadDataSources();
		Connection connectionRead = null;
		if (readDataSource != null) {
			connectionRead = readDataSource[iFeed % readDataSource.length].getConnection();
			connectionRead.setAutoCommit(true);
		}
		return connectionRead;
	}

	public static Connection getRandomWriteConnection() throws SQLException {

		int iFeed = (int) (Math.random() * 1000);
		DataSource[] writeDataSource = getWriteDataSources();
		Connection connectionWrite = null;
		if (writeDataSource != null) {
			connectionWrite = writeDataSource[iFeed % writeDataSource.length].getConnection();
			connectionWrite.setAutoCommit(true);
		}
		return connectionWrite;
	}

	/**
	 * 列表查询
	 * 
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	public static List<Map<String, Object>> queryList(Connection conn, String strSqlCommand, Object... parameters)
			throws SQLException {
		List<Map<String, Object>> result = null;
		QueryRunner runner = new QueryRunner();
		MapListHandler handler = new MapListHandler();
		result = runner.query(conn, strSqlCommand, handler, parameters);
		return result;
	}

	/***
	 * 分页查询1 废了
	 * 
	 * @param conn
	 * @param request
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws java.sql.SQLException
	 */
	/*
	 * @Deprecated public static Map<String, Object> queryPage(Connection conn,
	 * HttpServletRequest request, String sql, Object... parameters) throws
	 * SQLException { int pageNum = 1; int pageSize = 15; if
	 * (StringUtils.hasText(request.getParameter("pageNum"))) { pageNum =
	 * Integer.parseInt(request.getParameter("pageNum")); } if
	 * (StringUtils.hasText(request.getParameter("pageSize"))) { pageSize =
	 * Integer.parseInt(request.getParameter("pageSize")); } int start = (pageNum -
	 * 1) * pageSize; return null; //queryPage(conn, sql, start, pageSize,
	 * parameters); }
	 */

	public static long countCustomQuery(Connection conn, List<String> listClause, Object... parameters)
			throws SQLException {
		int iClauseCount = 0;
		String strNullOfPiontString = "Clause list is null";
		if (null == listClause) {
			throw new NullPointerException(strNullOfPiontString);

		} else {
			iClauseCount = listClause.size();
			if (0 == iClauseCount) {
				throw new NullPointerException(strNullOfPiontString);
			}
		}

		StringBuilder sbSqlCommand = new StringBuilder();
		sbSqlCommand.append(" SELECT COUNT(1) `count` ");

		for (int iClause = 0; iClause < iClauseCount; iClause++) {
			sbSqlCommand.append(" ");
			sbSqlCommand.append(listClause.get(iClause));
			if (iClause == iClauseCount - 1) {// 最后一个子句
				sbSqlCommand.append(" ");
			}
		}

		String strSqlCommand = sbSqlCommand.toString();

		QueryRunner runner = new QueryRunner();
		MapHandler handler = new MapHandler();
		Map<String, Object> mapResult = runner.query(conn, strSqlCommand, handler, parameters);

		return mapResult.get("count") == null ? 0 : Integer.parseInt(mapResult.get("count").toString());

	}

	public static long countCustomQuery(Connection conn, String strClause, Object... parameters) throws SQLException {
		// int iClauseCount=0;
		String strNullOfPiontString = "Clause list is null";
		if (null == strClause) {
			throw new NullPointerException(strNullOfPiontString);

		}
		/*
		 * else { iClauseCount=listClause.size(); if (0==iClauseCount){ throw new
		 * NullPointerException(strNullOfPiontString); } }
		 */

		StringBuilder sbSqlCommand = new StringBuilder();
		sbSqlCommand.append(" SELECT COUNT(1) `count` ").append(strClause).append(" ");
		;

		String strSqlCommand = sbSqlCommand.toString();

		QueryRunner runner = new QueryRunner();
		MapHandler handler = new MapHandler();
		Map<String, Object> mapResult = runner.query(conn, strSqlCommand, handler, parameters);

		return mapResult.get("count") == null ? 0 : Integer.parseInt(mapResult.get("count").toString());

	}

	/**
	 * 复杂自定义查询(为了针对 嵌套、子查询、复杂报表结构等状况)
	 * 
	 * @param conn
	 * @param sql
	 * @param start
	 * @param end
	 * @param parameters
	 * @return
	 * @throws java.sql.SQLException
	 */
	public static QueryResult<?> doCustomQuery(Connection conn, List<String> listProjection, List<String> listClause,
			PagerInfo pi, Object... parameters) throws SQLException {

		long lResultCount = DruidUtil.countCustomQuery(conn, listClause, parameters);

		if (lResultCount != 0) {

			StringBuilder sbSqlCommand = new StringBuilder(" SELECT ");

			if (null == listProjection) { // 字段列表null
				sbSqlCommand.append(" * ");
			} else {
				int iProjectCount = listProjection.size();
				if (0 >= iProjectCount) { // 没填字段
					sbSqlCommand.append(" * ");
				} else {
					// 处理投影字段
					for (int iProjectField = 0; iProjectField < iProjectCount; iProjectField++) {
						sbSqlCommand.append(" `");
						sbSqlCommand.append(listProjection.get(iProjectField));
						sbSqlCommand.append("` ");
						if (iProjectField < iProjectCount - 1) {
							sbSqlCommand.append(" , ");
						}

					}
				}

			}

			// 子句 筛选部分会包含? 参数依次使用 parameters
			// listClause为null或者为0会在上方count的时候出0的结果或异常，此处不做检测
			int iClauseCount = listClause.size();
			for (int iClause = 0; iClause < iClauseCount; iClause++) {
				sbSqlCommand.append(" ");
				sbSqlCommand.append(listClause.get(iClause));
				if (iClause == iClauseCount - 1) {// 最后一个子句
					sbSqlCommand.append(" ");
				}
			}

			// 分页 MySQL的Limit不需要Order子句吗？ 其他数据库呢
			if (null != pi) {
				sbSqlCommand.append(" LIMIT ");
				sbSqlCommand.append(pi.getStart());
				sbSqlCommand.append(pi.getLimit());

			} else {
				sbSqlCommand.append(" LIMIT ");
				sbSqlCommand.append(0);
				sbSqlCommand.append(500);// 默认数量500 防止过量查询

			}

			// 开始查询
			String strSqlCommand = sbSqlCommand.toString();
			List<Map<String, Object>> listResult = DruidUtil.queryList(conn, strSqlCommand, parameters);

			return new QueryResult<Map<String, Object>>(listResult, lResultCount);

		} else {

			return new QueryResult<Map<String, Object>>(null, 0);

		}

	}

	/**
	 * 新增的doCustomQuery @Title: doCustomQuery @Description:
	 * 新增的doCustomQuery @param @param conn @param @param strProjection @param @param
	 * strClause @param @param pi @param @param
	 * parameters @param @return @param @throws SQLException 设定文件 @return
	 * QueryResult<?> 返回类型 @throws
	 */
	public static QueryResult<?> doCustomQuery(Connection conn, String strProjection, String strClause, PagerInfo pi,
			Object... parameters) throws SQLException {

		long lResultCount = DruidUtil.countCustomQuery(conn, strClause, parameters);

		if (lResultCount != 0) {

			StringBuilder sbSqlCommand = new StringBuilder(" SELECT ");

			if (null == strProjection) { // 字段列表null
				sbSqlCommand.append("* ");
			} else {
				sbSqlCommand.append(strProjection).append(" ");

			}

			// 子句 筛选部分会包含? 参数依次使用 parameters
			// listClause为null或者为0会在上方count的时候出0的结果或异常，此处不做检测
			if (null == strClause) {
				throw new SQLException("No Clause body");
			} else {
				sbSqlCommand.append(strClause).append(" ");

			}

			// 分页 MySQL的Limit不需要Order子句吗？ 其他数据库呢
			if (null != pi) {
				sbSqlCommand.append(" LIMIT ");
				sbSqlCommand.append(pi.getStart());
				sbSqlCommand.append(pi.getLimit());

			} else {
				sbSqlCommand.append(" LIMIT ");
				sbSqlCommand.append(0);
				sbSqlCommand.append(500);// 默认数量500 防止过量查询

			}

			// 开始查询
			String strSqlCommand = sbSqlCommand.toString();
			List<Map<String, Object>> listResult = DruidUtil.queryList(conn, strSqlCommand, parameters);

			return new QueryResult<Map<String, Object>>(listResult, lResultCount);

		} else {

			return new QueryResult<Map<String, Object>>(null, 0);

		}

	}

	/**
	 * 新版标准查询工具（通用情况考虑，无法支持子查询和复杂嵌套查询，复杂情况请使用doCustomQuery）
	 * 
	 * @param listProjection
	 * 
	 * 
	 * @return Map<String,Object>
	 * @throws java.sql.SQLException
	 */
	public static QueryResult<Map<String, Object>> doQuery(Connection conn, List<String> listProjection,
			String strFromClause, String strWhereClause, List<SortInfo> listSortInfo, PagerInfo pi,
			Object... parameters) throws SQLException {

		// 这里会检查 From子句 是否为空
		long lResultCount = DruidUtil.countQuery(conn, strFromClause, strWhereClause, parameters);

		if (lResultCount != 0) {

			StringBuilder sbSqlCommand = new StringBuilder(" SELECT ");

			if (null == listProjection) { // 字段列表null
				sbSqlCommand.append(" * ");
			} else {
				int iProjectCount = listProjection.size();
				if (0 >= iProjectCount) { // 没填字段
					sbSqlCommand.append(" * ");
				} else {
					// 处理投影字段
					for (int iProjectField = 0; iProjectField < iProjectCount; iProjectField++) {
						// 这里注释掉的代码是要考虑 `表名`.`字段`这种形式的语句
						// 暂时自行加上
						// String strField=listProjection.get(iProjectField);
						// int iPointPosition=strField.indexOf(".");
						// if (iPointPosition!=-1){
						// if
						// }
						// String strFieldSegments [] =strField.split(".");

						// sbSqlCommand.append(" `");

						// 这里未对SQL关键字加``，也未根据'.'情况考虑
						// 请使用者自行保持素质
						sbSqlCommand.append(listProjection.get(iProjectField));
						// sbSqlCommand.append("` ");
						if (iProjectField < iProjectCount - 1) {
							sbSqlCommand.append(",");
						}

					}
				}

			}

			// 表和连接，筛选部分会包含? 参数依次使用 parameters

			// 表和连接
			sbSqlCommand.append(" ");
			sbSqlCommand.append(strFromClause);// 请直接在外填写（如何防止注入）

			// 筛选
			sbSqlCommand.append(" ");
			if (null != strWhereClause && !strWhereClause.isEmpty()) {
				sbSqlCommand.append(strWhereClause);
			}
			sbSqlCommand.append(" ");

			// 排序
			if (null != listSortInfo) {
				int iSortCount = listSortInfo.size();

				if (iSortCount > 0) {
					sbSqlCommand.append(" ORDER BY ");
					for (int iSortInfo = 0; iSortInfo < iSortCount; iSortInfo++) {

						SortInfo si = listSortInfo.get(iSortInfo);
						// sbSqlCommand.append(" `");
						sbSqlCommand.append(si.getSortField());
						// sbSqlCommand.append("` ");
						SortDirection sdDirection = si.getDirection();
						if (null == sdDirection) {
							sbSqlCommand.append(" ASC ");
						} else if (sdDirection == SortDirection.Ascending) {
							sbSqlCommand.append(" ASC ");
						} else if (sdDirection == SortDirection.Descending) {
							sbSqlCommand.append(" DESC ");
						}

						if (iSortInfo < iSortCount - 1) {
							sbSqlCommand.append(",");
						}
					}
				}
			}

			// 分页
			if (null != pi) {
				sbSqlCommand.append(" LIMIT ");
				sbSqlCommand.append(pi.getStart());
				sbSqlCommand.append(",");
				sbSqlCommand.append(pi.getLimit());

			} else {
				sbSqlCommand.append(" LIMIT ");
				sbSqlCommand.append(0);
				sbSqlCommand.append(",");
				sbSqlCommand.append(200);// 默认数量500 防止过量查询

			}

			// 开始查询
			String strSqlCommand = sbSqlCommand.toString();
			List<Map<String, Object>> listResult = DruidUtil.queryList(conn, strSqlCommand, parameters);
			if (null == listResult) {
				listResult = new ArrayList<Map<String, Object>>();
			}
			return new QueryResult<Map<String, Object>>(listResult, lResultCount);

		} else {

			return new QueryResult<Map<String, Object>>(new ArrayList<Map<String, Object>>(), 0);

		}

	}

	/**
	 * @param conn
	 * @param
	 * 
	 */

	public static QueryResult<Map<String, Object>> doQuery(Connection conn, List<String> listProjection,
			String strFromClause, String strWhereClause, SortInfo si, PagerInfo pi, Object... parameters)
			throws SQLException {

		List<SortInfo> listSortInfo = new ArrayList<SortInfo>();
		listSortInfo.add(si);
		return doQuery(conn, listProjection, strFromClause, strWhereClause, listSortInfo, pi, parameters);
	}

	public static QueryResult<Map<String, Object>> doQuery(Connection conn, String strProjection, String strFromClause,
			String strWhereClause, List<SortInfo> listSortInfo, PagerInfo pi, Object... parameters)
			throws SQLException {

		// 如果字符串中有逗号，识别一下解决
		String[] arrayProjects = strProjection.split(",");
		List<String> listProjection = new ArrayList<String>();
		for (int iProject = 0; iProject < arrayProjects.length; iProject++) {
			String strProjectString = arrayProjects[iProject].trim();
			if (strProjection != null && !strProjectString.isEmpty()) {
				listProjection.add(strProjectString);
			}
		}
		return doQuery(conn, listProjection, strFromClause, strWhereClause, listSortInfo, pi, parameters);
	}

	public static QueryResult<Map<String, Object>> doQuery(Connection conn, String strProjection, String strFromClause,
			String strWhereClause, SortInfo si, PagerInfo pi, Object... parameters) throws SQLException {

		List<SortInfo> listSortInfo = new ArrayList<SortInfo>();
		listSortInfo.add(si);
		return doQuery(conn, strProjection, strFromClause, strWhereClause, listSortInfo, pi, parameters);
	}

	/**
	 * 查询唯一结果集
	 * 
	 * @param conn
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws java.sql.SQLException
	 */
	public static Map<String, Object> queryUniqueResult(Connection conn, String strSqlCommand, Object... parameters)
			throws SQLException {
		QueryRunner runner = new QueryRunner();
		MapHandler handler = new MapHandler();
		return runner.query(conn, strSqlCommand, handler, parameters);
	}

	/***
	 * 查询数据总行数(已经更新)
	 * 
	 * @param conn
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws java.sql.SQLException
	 */
	public static long countQuery(Connection conn, String strFromClause, String strWhereClause, Object... parameters)
			throws SQLException {

		if (null == strFromClause || strFromClause.isEmpty())
			throw new NullPointerException("From Clause is null");

		StringBuilder sbSqlCommand = new StringBuilder();
		sbSqlCommand.append(" SELECT COUNT(1) `count` ").append(strFromClause).append(" ");

		if (null != strWhereClause && !strWhereClause.isEmpty()) {
			sbSqlCommand.append(strWhereClause);
		}
		QueryRunner runner = new QueryRunner();
		MapHandler handler = new MapHandler();
		Map<String, Object> mapResult = runner.query(conn, sbSqlCommand.toString(), handler, parameters);

		return mapResult.get("count") == null ? 0 : Integer.parseInt(mapResult.get("count").toString());

		/*
		 * 历史代码 strSqlCommand = "SELECT COUNT(0) count FROM " + strSqlCommand.replaceAll
		 * (".*?(?i)FROM\\s","").replaceAll("\\s(?i)ORDER\\s+BY.*$"
		 * ,"").replaceAll("\\s(?i)group\\s+by.*$",""); QueryRunner runner= new
		 * QueryRunner(); MapHandler handler = new MapHandler(); Map result =
		 * runner.query(conn,strSqlCommand,handler,parameters); return
		 * result.get("count") == null ? 0 :
		 * Integer.parseInt(result.get("count").toString());
		 */

	}

	/**
	 * 列表查询
	 * 
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	public static Map<String, Object> findById(Connection conn, String objectName, String strPrimaryKeyName,
			String strPrimaryKeyValue) throws SQLException {
		Map<String, Object> result = null;
		QueryRunner runner = new QueryRunner();
		MapHandler handler = new MapHandler();
		String sql = "select * from " + objectName + " where `" + strPrimaryKeyName + "` = ? ";
		result = runner.query(conn, sql, handler, strPrimaryKeyValue);
		return result;
	}

	/***
	 * 按主键删除数据
	 * 
	 * @param conn
	 * @param objectName
	 * @param id
	 * @return
	 */
	public static int deleteById(Connection conn, String objectName, String id) throws SQLException {
		String sql = "DELETE FROM " + objectName + " WHERE id = ? ";
		QueryRunner runner = new QueryRunner();
		return runner.update(conn, sql, id);
	}

	/***
	 * SQL语句执行
	 * 
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws java.io.IOException
	 * @throws java.sql.SQLException
	 */
	public static long doExecute(Connection conn, String sql, Object... parameters) throws SQLException {
		QueryRunner runner = new QueryRunner();
		return runner.update(conn, sql, parameters);
	}

	/***
	 * 数据保存
	 * 
	 * @param conn
	 * @param data
	 * @param objectName
	 * @return
	 * @throws java.sql.SQLException
	 */
	public static <T> T save(Connection conn, Map<String, Object> mapData, String objectName/* ,Class<T> cls */)
			throws SQLException {
		String dateStr = DateUtil.fomatDate(new Date(), "yyyy-MM-dd HH:mm:ss");
		if (!mapData.containsKey("update_time") || mapData.get("update_time") == null) {
			mapData.put("update_time", dateStr);
		}
		if (!mapData.containsKey("create_time") || mapData.get("create_time") == null) {
			mapData.put("create_time", dateStr);
		}
		Iterator<String> it = mapData.keySet().iterator();

		String key = "";
		StringBuilder sbInsert = new StringBuilder();
		sbInsert.append("INSERT INTO ").append(objectName);

		List<Object> valueList = new ArrayList<Object>();
		List<String> filedNameList = new ArrayList<String>();
		List<String> preValueList = new ArrayList<String>();

		while (it.hasNext()) {
			key = it.next().toString();
			filedNameList.add(key);
			preValueList.add("?");
			valueList.add(mapData.get(key));
		}
		sbInsert.append(" ( `").append(StringUtils.collectionToDelimitedString(filedNameList, "`,`"))
				.append("` ) VALUES ( ").append(StringUtils.collectionToDelimitedString(preValueList, ",")).append(")");
		QueryRunner run = new QueryRunner();

		// return run.insert(conn, insertSql.toString(), Statement.RETURN_GENERATED_KEYS
		// ,valueList.toArray());
		// return run.update(conn, insertSql.toString(), valueList.toArray());

		return run.insert(conn, sbInsert.toString(), new InsertResultHandler<T>(), valueList.toArray());
	}

	/*** 批量保存 ***/
	public static int[] batchSave(Connection conn, List<Map<String, Object>> listData, String objectName)
			throws SQLException {
		if (CollectionUtils.isEmpty(listData)) {
			return null;
		}
		String dateStr = DateUtil.fomatDate(new Date(), "yyyy-MM-dd HH:mm:ss");
		Object[][] paramValues = new Object[listData.size()][listData.get(0).keySet().size() + 2];
		Map<String, Object> filedData = null;
		String key = "";
		// 合成数据
		for (int i = 0, len = listData.size(); i < len; i++) {
			filedData = listData.get(i);
			filedData.put("update_time", dateStr);
			if (!StringUtils.hasText(filedData.get("create_time"))) {
				filedData.put("create_time", dateStr);
			}
			Iterator<?> filedIt = filedData.keySet().iterator();
			int inx = 0;
			while (filedIt.hasNext()) {
				key = filedIt.next().toString();
				paramValues[i][inx] = filedData.get(key);
				inx++;
			}
		}
		// 合成插入语句
		StringBuffer insertSql = new StringBuffer(128);
		insertSql.append("insert into ").append(objectName);
		List<String> filedNameList = new ArrayList<String>();
		List<String> preValueList = new ArrayList<String>();
		Iterator<?> it = listData.get(0).keySet().iterator();
		while (it.hasNext()) {
			key = it.next().toString();
			filedNameList.add(key);
			preValueList.add("?");
		}
		insertSql.append(" ( ").append(StringUtils.collectionToDelimitedString(filedNameList, ","))
				.append(" ) values ( ").append(StringUtils.collectionToDelimitedString(preValueList, ",")).append(")");
		QueryRunner run = new QueryRunner();
		return run.batch(conn, insertSql.toString(), paramValues);
	}

	/**
	 * 数据批量更新
	 * 
	 * @param conn
	 * @param dataList
	 * @param objectName
	 * @return
	 * @throws java.sql.SQLException
	 */
	public static int[] batchUpdate(Connection conn, List<Map<String, Object>> dataList, String objectName)
			throws SQLException {
		if (CollectionUtils.isEmpty(dataList)) {
			return null;
		}
		String dateStr = DateUtil.fomatDate(new Date(), "yyyy-MM-dd HH:mm:ss");
		Map<String, Object> data = null;
		Object[][] paramValues = new Object[dataList.size()][1];
		for (int i = 0, len = dataList.size(); i < len; i++) {
			data = dataList.get(i);
			data.put("update_time", dateStr);
			paramValues[i][1] = data.get("ID");
		}
		Iterator<String> it = dataList.get(0).keySet().iterator();
		String key = "";
		StringBuffer updateSql = new StringBuffer(128);
		updateSql.append("UPDATE ").append(objectName).append(" SET ");
		while (it.hasNext()) {
			key = it.next().toString();
			if (key.equalsIgnoreCase("id")) {
				continue;
			}
			updateSql.append(key).append("=?,");
		}
		updateSql.deleteCharAt(updateSql.length() - 1);
		updateSql.append(" WHERE id= ? ");
		QueryRunner run = new QueryRunner();
		return run.batch(conn, updateSql.toString(), paramValues);
	}

	/***
	 * 数据更新
	 * 
	 * @param conn
	 * @param data
	 * @param objectName
	 * @return
	 * @throws java.sql.SQLException
	 */
	public static long update(Connection conn, Map<String, Object> mapData, String objectName, String strPrimaryKey)
			throws SQLException {
		String dateStr = DateUtil.fomatDate(new Date(), "yyyy-MM-dd HH:mm:ss");

		if (!mapData.containsKey("update_time") || mapData.get("update_time") == null) {
			mapData.put("update_time", dateStr);
		}

		Iterator<String> it = mapData.keySet().iterator();
		String key = "";
		List<Object> valList = new ArrayList<Object>();
		StringBuffer updateSql = new StringBuffer(128);
		updateSql.append("UPDATE ").append(objectName).append(" SET ");
		while (it.hasNext()) {
			key = it.next().toString();
			if (key.equalsIgnoreCase(strPrimaryKey)) {
				continue;
			}
			updateSql.append(key).append("=?,");
			valList.add(mapData.get(key));
		}
		updateSql.deleteCharAt(updateSql.length() - 1);// 删掉最后一个","
		updateSql.append(" WHERE `" + strPrimaryKey + "`= ? ");
		valList.add(mapData.get(strPrimaryKey));
		return doExecute(conn, updateSql.toString(), valList.toArray());
	}

	public static void beginTransaction(Connection connection) throws Exception {
		connection.setAutoCommit(false);
		connection.setReadOnly(false);
	}

	public static void commitTransaction(Connection connection) throws SQLException {
		if (!connection.getAutoCommit()) {
			connection.commit();
		}
	}

	public static void rollbackTransaction(Connection connection) throws SQLException {
		if (!connection.getAutoCommit()) {
			connection.rollback();
		}
	}

	/**
	 * Close a <code>Connection</code>, avoid closing if null.
	 * 
	 * @param conn
	 *            Connection to close.
	 * @throws java.sql.SQLException
	 *             if a database access error occurs
	 */
	public static void close(Connection conn) {
		if (conn != null) {
			try {
				if (!conn.isClosed()) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static class InsertResultHandler<T> implements ResultSetHandler<T> {

		@Override
		public T handle(ResultSet rs) throws SQLException {
			T t = null;
			if (rs.next()) {
				t = (T) rs.getObject(1);
			} else {
				// throw new NoIdGeneratedException("No id generated from database...");
				return null;
			}

			return t;

		}

	}

}
