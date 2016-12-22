package com.chuanchen.util.sql;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chuanchen.util.string.StringCommonUtil;

public class DbCommonSelectOperation<T> {
	private static Logger logger = LoggerFactory.getLogger(DbCommonSelectOperation.class);
	private Connection connection;
	private String dbName;
	private List<String> selectRowNames = new ArrayList<>();
	private List<String> whereArgumentKeys = new ArrayList<>();
	private List<Object> whereArgumentValues = new ArrayList<>();
	private StringBuilder sqlBuilder = new StringBuilder();
	public String getDbName() {
		return dbName;
	}

	/**
	 * set database name
	 * 
	 * @param dbName
	 * @return
	 */
	public DbCommonSelectOperation<T> setDbName(String dbName) {
		this.dbName = dbName;
		return this;
	}

	/**
	 * set connection
	 * 
	 * @param connection
	 * @return
	 */
	public DbCommonSelectOperation<T> setConnection(Connection connection) {
		this.connection = connection;
		return this;
	}
	/**
	 * set selectRowNames(设置查询的列名:List)
	 * @param selectRowNames
	 * @return
	 */
	public DbCommonSelectOperation<T> setSelectRowNames(List<String> selectRowNames){
		this.selectRowNames.addAll(selectRowNames);
		return this;
	}
	/**
	 * set selectRowNames(设置查询的列名:String[])
	 * @param selectRowNames
	 * @return
	 */
	public DbCommonSelectOperation<T> setSelectRowNames(String[] selectRowNames){
		for(String selectRowName : selectRowNames){
			this.selectRowNames.add(selectRowName);
		}
		return this;
	}
	/**
	 * add selectRowNames(添加查询的列名:String)
	 * @param selectRowName
	 * @return
	 */
	public DbCommonSelectOperation<T> addSelectRowName(String selectRowName){
		this.selectRowNames.add(selectRowName);
		return this;
	}
	/**
     * set whereArgumentKeys(设置where子句的列名:List)
     * @param whereArgumentKeys
     * @return
     */
	public DbCommonSelectOperation<T> setWhereArgumentKeys(List<String> whereArgumentKeys) {
		this.whereArgumentKeys.addAll(whereArgumentKeys);
		return this;
	}
	/**
	 * set whereArgumentKeys(设置where子句的列名:String[])
	 * @param whereArgumentKeys
	 * @return
	 */
	public DbCommonSelectOperation<T> setWhereArgumentKeys(String[] whereArgumentKeys){
		for(String whereArgumentKey : whereArgumentKeys){
			this.whereArgumentKeys.add(whereArgumentKey);
		}
		return this;
	}
   /**
    * set whereArgumentValues(设置where子句的值:List)str
    * @param whereArgumentValues
    * @return
    */
	public DbCommonSelectOperation<T> setWhereArgumentValues(List<Object> whereArgumentValues) {
		this.whereArgumentValues.addAll(whereArgumentValues);
		return this;
	}
	/**
	 * set whereArgumentValues(设置where子句的值:String[])
	 * @param whereArgumentValues
	 * @return
	 */
	public DbCommonSelectOperation<T> setWhereArgumentValues(String[] whereArgumentValues){
		for(String whereArgumentValue : whereArgumentValues){
			this.whereArgumentValues.add(whereArgumentValue);
		}
		return this;
	}
   /**
    * add whereArgumentKeys(添加where子句的列名)
    * @param whereArgumentKey
    * @return
    */
	public DbCommonSelectOperation<T> addWhereArgumentKey(String whereArgumentKey) {
		whereArgumentKeys.add(whereArgumentKey);
		return this;
	}
    /**
     * add whereArgumentValues(添加where子句的值)
     * @param whereArgumentValue
     * @return
     */
	public DbCommonSelectOperation<T> addWhereArgumentValue(Object whereArgumentValue) {
		whereArgumentValues.add(whereArgumentValue);
		return this;
	}
	public T selectObject(Class<T> c){
		if(connection == null){
			logger.info("connection can't be null,please set connection!");
			return null;
		}
		if(StringCommonUtil.isBlank(dbName)){
			logger.info("dbName can't be null,please set dbName!");
			return null;
		}
		if(whereArgumentKeys.size() != whereArgumentValues.size()){
			logger.info("argumentKeys or argumentValues set error,please check it!");
			return null;
		}
		sqlBuilder.append("select ");
		if(selectRowNames.size() > 0){
			for(String selectRowName : selectRowNames){
				sqlBuilder.append(selectRowName+",");
			}
			sqlBuilder = new StringBuilder(sqlBuilder.substring(0, sqlBuilder.length() - 1));
			sqlBuilder.append(" from "+dbName);
		}else{
			sqlBuilder.append("* from "+dbName);
		}
		if(whereArgumentKeys.size() > 0){
			sqlBuilder.append(" where ");
			for(int i = 0;i < whereArgumentKeys.size();i++){
				String whereArgumentKey = whereArgumentKeys.get(i);
				if(i == 0){
					sqlBuilder.append(whereArgumentKey+" = ?");
				}else{
					sqlBuilder.append(" and "+whereArgumentKey+" = ?");
				}
			}
		}
		System.out.println(sqlBuilder.toString());
		PreparedStatement ps = null;
		ResultSet rs = null;
		T result = null;
		try {
			ps = connection.prepareStatement(sqlBuilder.toString());
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
			rs = ps.executeQuery();
			if(rs.next()){
				try {
					result = c.newInstance();
					Method[] methods = c.getDeclaredMethods();
					ResultSetMetaData metaData = rs.getMetaData();
					int column = metaData.getColumnCount();
				    for(int i = 1;i <= column;i++){
				    	String columnName = metaData.getColumnName(i);
				    	int type = metaData.getColumnType(i);
				    	for(Method method : methods){
							String fieldName = method.getName().substring(3);
							if(columnName.equalsIgnoreCase(fieldName)){
								switch (type) {
								case Types.VARCHAR:{
									String columnValue = rs.getString(i);
									try {
										method.invoke(result, columnValue);
									} catch (IllegalArgumentException e) {
										e.printStackTrace();
									} catch (InvocationTargetException e) {
										e.printStackTrace();
									}
									break;}
								case Types.INTEGER:{
									int columnValue = rs.getInt(i);
									try {
										method.invoke(result, columnValue);
									} catch (IllegalArgumentException e) {
										e.printStackTrace();
									} catch (InvocationTargetException e) {
										e.printStackTrace();
									}
									break;
								}
                                case Types.TIMESTAMP:{
                                	Timestamp columnValue = rs.getTimestamp(i);
                                	try {
										method.invoke(result, new Date(columnValue.getTime()));
									} catch (IllegalArgumentException e) {
										e.printStackTrace();
									} catch (InvocationTargetException e) {
										e.printStackTrace();
									}
                                	break;
                                }
								default:
									break;
								}
							}
						}	
				    }
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		} catch (SQLException e) {
			logger.error(e.toString());
		}finally {
			DbCommonUtil.closeAll(rs, ps, connection);
		}
		return result;
	}
}
