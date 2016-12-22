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

public class DbCommonInsertOperation {
	private static Logger logger = LoggerFactory.getLogger(DbCommonInsertOperation.class);
	private Connection connection;
	private String dbName;
	private List<String> argumentKeys = new ArrayList<>();
    private List<Object> argumentValues = new ArrayList<>();
    private StringBuilder sqlBuilder = new StringBuilder();
    public static DbCommonInsertOperation create(){
    	return new DbCommonInsertOperation();
    }
    private DbCommonInsertOperation(){}
    /**
     * set dbName
     * @param dbName String
     * @return
     */
	public DbCommonInsertOperation setDbName(String dbName) {
		this.dbName = dbName;
		return this;
	}
	/**
	 * set Connection
	 * @param connection
	 * @return
	 */
	public DbCommonInsertOperation setConnection(Connection connection){
		this.connection = connection;
		return this;
	}
	/**
	 * add ArgumentNames(添加参数名称:String[])
	 * @param argumentNames
	 * @return
	 */
    public DbCommonInsertOperation setArgumentKeys(String[] argumentKeys){
    	for(String argumentKey : argumentKeys){
    		this.argumentKeys.add(argumentKey);
    	}
    	return this;
    }
    /**
     * add argumentNames(添加参数名:List)
     * @param argumentNames
     * @return
     */
    public DbCommonInsertOperation setArgumentKeys(List<String> argumentKeys){
    	this.argumentKeys.addAll(argumentKeys);
    	return this;
    }
    /**
     * add argumentName(添加参数名:String)
     * @param argumentName
     * @return
     */
	public DbCommonInsertOperation addArgumentKey(String argumentKey) {
		if(StringCommonUtil.isBlank(argumentKey)){
			throw new NullPointerException("argumentName is null or empty!");
		}
		this.argumentKeys.add(argumentKey);
		return this;
	}
	/**
	 * add argumentValues(添加参数值:List)
	 * @param arguments
	 * @return
	 */
	public DbCommonInsertOperation setArgumentValues(List<Object> argumentValues){
	    this.argumentValues= argumentValues;
	    return this;
	}
	/**
	 * add argumentValue(添加参数值:Object)
	 * @param argument
	 * @return
	 */
    public DbCommonInsertOperation addArgumentValue(Object argumentValue){
    	if(argumentValue == null){
    		throw new NullPointerException("argument is null or empty!");
    	}
    	this.argumentValues.add(argumentValue);
    	return this;
    }
    /**
     * 插入数据操作
     * @return
     */
    public boolean insert(){
    	if(connection == null){
    		logger.info("connection is null,please set connection!");
    		return false;
    	}
    	if((argumentKeys.size() > 0 && argumentKeys.size() != argumentValues.size()) || argumentValues.size() <= 0){
    		logger.info("the arguments set error,please reset!");
    		return false;
    	}
    	if(StringCommonUtil.isBlank(dbName)){
    		logger.error("dbName is blank,please initlizal the dbName!");
    		return false;
    	}
    	sqlBuilder.append("insert into ");
    	sqlBuilder.append(dbName);
    	if(argumentKeys.size() > 0){
    		sqlBuilder.append("(");
    		for(String argumentName : argumentKeys){
    			sqlBuilder.append(argumentName + ",");	
        	}
    		sqlBuilder = new StringBuilder(sqlBuilder.substring(0, sqlBuilder.length() - 1));
    		sqlBuilder.append(")");
    	}
    	if(argumentValues.size() <= 0){
    		logger.error("arguments is null,please add arguments!");
    		return false;
    	}
    	sqlBuilder.append(" values(");
    	for(int i = 0;i < argumentValues.size();i++){
    		sqlBuilder.append("?,");
    	}
    	sqlBuilder = new StringBuilder(sqlBuilder.substring(0, sqlBuilder.length() - 1)+")");
    	System.out.println(sqlBuilder.toString());
        PreparedStatement ps = null;
        boolean result = false;
        try {
			ps = connection.prepareStatement(sqlBuilder.toString());
			for(int i = 1;i <= argumentValues.size();i++){
				Object argument = argumentValues.get(i - 1);
				if(argument instanceof String){
					ps.setString(i, (String) argument);
				}else if(argument instanceof Integer){
					ps.setInt(i, (int) argument);
				}else if(argument instanceof Date){
					ps.setTimestamp(i, new Timestamp(((Date)argument).getTime()));
				}
			}
			ps.execute();
			result = ps.getUpdateCount() > 0 ? true : false;
		} catch (SQLException e) {
			logger.error(e.toString());
		}finally {
			DbCommonUtil.closeAll(null, ps, connection);
		}
        return result;
    }
}
