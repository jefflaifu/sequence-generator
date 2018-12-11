package com.jeff.sequence.dao;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBHelper {
	
	private static Logger logger = LoggerFactory.getLogger(DBHelper.class);
	
	private DBOperation dbOperation;
	

	public DBHelper(DataSource dataSource)
	{
		this.dbOperation = new DBOperation();
		dbOperation.setDataSource(dataSource);
	}
	
	public void createTable() throws SQLException
	{
		String sql = "create table if not exists sequence_table(sequence_name varchar(50) not null, current_value bigint not null default 1, increment bigint not null default 1, primary key (sequence_name)) engine=innodb";
		try {
			dbOperation.create(sql);
		} catch (SQLException e) {
			logger.error("creating table failed!!!" + e.getMessage());
			throw e;
		}
	}
	
	public void createFunction() throws SQLException
	{
		String dropCurrentValueFunction = "drop function if exists current_value";
		String createCurrentValueFunction = "create function  current_value(seq_name varchar(50))\n" + 
				"returns bigint \n" + 
				"reads sql data\n" + 
				"begin\n" + 
				"declare value bigint ;\n" + 
				"set value = 0;\n" + 
				"select current_value into value from sequence_table where sequence_name = seq_name;\n" + 
				"return value ;\n" + 
				"end ;";
		String dropNextValueFunction = "drop function if exists next_value";
		String createNextValueFunction = "create function  next_value(seq_name varchar(50), cache bigint)\n" + 
				"returns bigint \n" + 
				"reads sql data\n" + 
				"begin\n" + 
				"update sequence_table set current_value = current_value + increment * cache  where sequence_name = seq_name;\n" + 
				"return current_value(seq_name) ;\n" + 
				"end ;";
		try {
			dbOperation.create(dropCurrentValueFunction);
			dbOperation.create(createCurrentValueFunction);
			dbOperation.create(dropNextValueFunction);
			dbOperation.create(createNextValueFunction);
		} catch (SQLException e) {
			logger.error("creating function failed!!!" + e.getMessage());
			throw e;
		}
	}
	
	public void createSequence(String sequenceName,int step) throws SQLException
	{
		String sql = "select 1 as value from sequence_table where sequence_name = ?";
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("sequence_name", sequenceName);
		Long result = dbOperation.query(sql, map);
		if (new Long(0).equals(result))
		{
			sql = "insert into sequence_table(sequence_name,increment) values(?," + step + ")";
			Long update = dbOperation.update(sql, map);
		}
		else
		{
			sql = "update sequence_table set increment=" + step + " where sequence_name = ?";
			Long update = dbOperation.update(sql, map);
		}
	}
	
	
	public  Long next(String sequenceName, int cache) throws SQLException {
		String sql = "select next_value(?," + cache + ") as value";
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("sequence_name", sequenceName);
		Long result = dbOperation.query(sql, map);
		return result;
	}
	
	
	
}
