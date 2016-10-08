package com.gslab.com.flink.jdbc.connector.consumer;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

public class JDBCFetcher<T> implements Runnable {

	private final SourceContext<T> sourceContext;

	private final RuntimeContext runtimeContext;

	private final DeserializationSchema<T> deserializer;
	
	private Properties  consumerProperties;
	private transient Connection dbConn;
	private transient Statement statement;
	private static Logger LOGGER = LoggerFactory.getLogger(JDBCFetcher.class);


	public void checkForValidConsumerProperties(Properties props){
		Preconditions.checkNotNull(props.get(ConsumerProperties.JDBC_DRIVER_NAME), "Illegal Argument passed: JDBC Driver Name is Null.");
		Preconditions.checkNotNull(props.get(ConsumerProperties.DB_URL), "Illegal Argument passed: JDBC DB URl is Null.");
		Preconditions.checkNotNull(props.get(ConsumerProperties.USERNAME), "Illegal Argument passed: JDBC Username is Null.");
		Preconditions.checkNotNull(props.get(ConsumerProperties.PASSWORD), "Illegal Argument passed: JDBC Password is Null.");
		Preconditions.checkNotNull(props.get(ConsumerProperties.SQL_QUERY), "Illegal Argument passed: JDBC Driver Sql query is Null.");
	}
	
	public JDBCFetcher(SourceContext<T> sourceContext, StreamingRuntimeContext runtimeContext, DeserializationSchema<T> deserializer, Properties props) throws ClassNotFoundException, SQLException {
		checkForValidConsumerProperties(props);
		this.sourceContext = sourceContext;
		this.runtimeContext = runtimeContext;
		this.deserializer = deserializer;
		this.consumerProperties = props;
		try{
			LOGGER.info("loading driver class.");
			Class.forName(props.getProperty(ConsumerProperties.JDBC_DRIVER_NAME));
			LOGGER.info("Connecting to a selected database...");
			dbConn = (Connection) DriverManager.getConnection(props.getProperty(ConsumerProperties.DB_URL), props.getProperty(ConsumerProperties.USERNAME), props.getProperty(ConsumerProperties.PASSWORD));
			LOGGER.info("Connected database successfully.");
			LOGGER.info("Creating statement.");
			statement = (Statement) dbConn.createStatement();
		}
		 catch (ClassNotFoundException e) {
			throw e;
		}
		catch (SQLException e) {
			throw e;
		}
	}

	public void runFetchLoop() throws Exception {
		Thread runner = new Thread(this, "JDBC Fetcher for " + this.runtimeContext.getTaskNameWithSubtasks());
		runner.setDaemon(true);
		runner.start();
		try {
			runner.join();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	protected final void emitRecord(T record) {
		sourceContext.collect(record);
	}

	public void run(){
		try {
			ResultSet rs = statement.executeQuery(this.consumerProperties.getProperty(ConsumerProperties.SQL_QUERY));
			while (rs.next()) {
				T value = deserializer.deserialize(rs);
				emitRecord(value);
			}
			rs.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement != null)
					dbConn.close();
			} catch (SQLException se) {
			}
			try {
				if (dbConn != null)
					dbConn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}

}
