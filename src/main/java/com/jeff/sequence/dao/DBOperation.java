package com.jeff.sequence.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

public class DBOperation {
	
	private DataSource dataSource;
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public Long query(String sql, Map<String, Object> map) throws SQLException
	{
		Connection connection = null;
		PreparedStatement prepareStatement = null;
		ResultSet resultSet =  null;
		try 
		{
			connection = dataSource.getConnection();
			prepareStatement = connection.prepareStatement(sql);
			int i = 1;
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				prepareStatement.setObject(i++, entry.getValue());
			}
			resultSet = prepareStatement.executeQuery();
			if (resultSet.next())
			{
				return resultSet.getLong("value");
			}
			else
			{
				return 0L;
			}
		}
		finally
		{
			if (resultSet != null && !resultSet.isClosed())
			{
				resultSet.close();
			}
			if (prepareStatement != null && !prepareStatement.isClosed())
			{
				prepareStatement.close();
			}
			
			if (connection != null && !connection.isClosed())
			{
				connection.close();
			}
		}
	}
	
	public Long update(String sql, Map<String, Object> map) throws SQLException
	{
		Connection connection = null;
		PreparedStatement prepareStatement = null;
		ResultSet resultSet =  null;
		try 
		{
			connection = dataSource.getConnection();
			prepareStatement = connection.prepareStatement(sql);
			int i = 1;
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				prepareStatement.setObject(i++, entry.getValue());
			}
			return Long.valueOf(prepareStatement.executeUpdate());
		}
		finally
		{
			if (resultSet != null && !resultSet.isClosed())
			{
				resultSet.close();
			}
			if (prepareStatement != null && !prepareStatement.isClosed())
			{
				prepareStatement.close();
			}
			
			if (connection != null && !connection.isClosed())
			{
				connection.close();
			}
		}
	}
	
	
	public void create(String sql) throws SQLException
	{
		Connection connection = null;
		PreparedStatement prepareStatement = null;
		try 
		{
			connection = dataSource.getConnection();
			prepareStatement = connection.prepareStatement(sql);
			prepareStatement.execute();
		}
		finally
		{
			if (prepareStatement != null && !prepareStatement.isClosed())
			{
				prepareStatement.close();
			}
			
			if (connection != null && !connection.isClosed())
			{
				connection.close();
			}
		}
	}
}
