package com.chuanchen.util.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chuanchen.util.string.StringCommonUtil;


public class DbCommonDeleteOperation {
	private static Logger logger = LoggerFactory.getLogger(DbCommonDeleteOperation.class);
	private String dbName;
	private Connection connection;
	private List<String> whereArgumentKeys = new ArrayList<>();
	private List<Object> whereArgumentValues = new ArrayList<>();
    private StringBuilder sqlBuilder = new StringBuilder();
    public static DbCommonDeleteOperation create(){
    	return new DbCommonDeleteOperation();
    }
    private DbCommonDeleteOperation(){}
	public String getDbName() {
		return dbName;
	}

	/**
	 * set database name
	 * 
	 * @param dbName
	 * @return
	 */
	public DbCommonDeleteOperation setDbName(String dbName) {
		this.dbName = dbName;
		return this;
	}

	/**
	 * set connection
	 * 
	 * @param connection
	 * @return
	 */
	public DbCommonDeleteOperation setConnection(Connection connection) {
		this.connection = connection;
		return this;
	}
    /**
     * set whereArgumentKeys(设置where子句的列名)
     * @param whereArgumentKeys
     * @return
     */
	public DbCommonDeleteOperation setWhereArgumentKes(List<String> whereArgumentKeys) {
		this.whereArgumentKeys = whereArgumentKeys;
		return this;
	}
   /**
    * set whereArgumentValues(设置where子句的值)
    * @param whereArgumentValues
    * @return
    */
	public DbCommonDeleteOperation setWhereArgumentValues(List<Object> whereArgumentValues) {
		this.whereArgumentValues = whereArgumentValues;
		return this;
	}
   /**
    * add whereArgumentKeys(添加where子句的列名)
    * @param whereArgumentKey
    * @return
    */
	public DbCommonDeleteOperation addWhereArgumentKey(String whereArgumentKey) {
		whereArgumentKeys.add(whereArgumentKey);
		return this;
	}
    /**
     * add whereArgumentValues(添加where子句的值)
     * @param whereArgumentValue
     * @return
     */
	public DbCommonDeleteOperation addWhereArgumentValue(Object whereArgumentValue) {
		whereArgumentValues.add(whereArgumentValue);
		return this;
	}
	/**
	 * 删除操作(根据返回值判断是否删除成功)
	 * @return
	 */
	public boolean delete(){
		if(connection == null){
			logger.info("connection can't be null,please set connection!");
			return false;
		}
		if(StringCommonUtil.isBlank(dbName)){
			logger.info("dbName can't be null,please set dbName!");
			return false;
		}
		if(whereArgumentKeys.size() > 0 && whereArgumentKeys.size() != whereArgumentValues.size()){
			logger.info("argumentKeys or argumentValues set error,please check it!");
			return false;
		}
		sqlBuilder.append("delete from "+dbName);
		for(int i = 0;i < whereArgumentKeys.size();i++){
			String whereArgumentKey = whereArgumentKeys.get(i);
			if(i == 0){
				sqlBuilder.append(" "+whereArgumentKey+" = ?");
			}else{
				sqlBuilder.append(" and "+whereArgumentKey+" = ?");
			}
		}
		PreparedStatement ps = null;
		boolean result = false;
		try {
			ps = this.connection .prepareStatement(sqlBuilder.toString());
			for(int i = 1;i <= whereArgumentValues.size();i++){
			    Object argumentValue = whereArgumentValues.get(i);
				if(argumentValue instanceof String){
					ps.setString(i, (String) argumentValue);
				}else if(argumentValue instanceof Integer){
					ps.setInt(i, (int) argumentValue);
				}else if(argumentValue instanceof Date){
					ps.setTimestamp(i, new Timestamp(((Date)argumentValue).getTime()));
				}
			}
			ps.executeUpdate();
			result = ps.getUpdateCount() > 0 ? true : false;
		} catch (SQLException e) {
			logger.error(e.toString());
		}finally {
			DbCommonUtil.closeAll(null, ps, connection);
		}
		return result;
	}
}
