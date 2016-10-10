package com.gslab.com.flink.jdbc.connector.querier;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.gslab.com.flink.jdbc.connector.consumer.JdbcSourceConnectorConfig;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;

public abstract class AbstractQuerier<T> implements Querier<T>{

	protected SourceContext<T> sourceContext;
	protected final DeserializationSchema<T> deserializer;
	protected Properties consProp;
	protected transient Connection dbConn;
	protected PreparedStatement stmt;
	protected Object checkpointLock;
	protected volatile boolean isRunning = true;
	private static Logger LOGGER = LoggerFactory.getLogger(AbstractQuerier.class);

	public AbstractQuerier(DeserializationSchema<T> deserializer, Properties props) throws ClassNotFoundException, SQLException {
		checkForValidConsumerProperties(props);
		this.deserializer = deserializer;
		this.consProp = props;
	}
	
	public void openConnection() throws ClassNotFoundException, SQLException{
		LOGGER.info("loading driver class.");
		Class.forName(this.consProp.getProperty(JdbcSourceConnectorConfig.JDBC_DRIVER_NAME));
		
		LOGGER.info("Connecting to a selected database...");
		dbConn = (Connection) DriverManager.getConnection(this.consProp.getProperty(JdbcSourceConnectorConfig.DB_URL), this.consProp.getProperty(JdbcSourceConnectorConfig.USERNAME), this.consProp.getProperty(JdbcSourceConnectorConfig.PASSWORD));
		LOGGER.info("Connected database successfully.");
		
		LOGGER.info("Creating statement.");
		stmt = createPreparedStatement();
	}

	public void closeConnection() {
		try {
			if (stmt != null)
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
	
	public void cancel() {
		isRunning = false;
	}

	protected abstract PreparedStatement createPreparedStatement() throws SQLException;
	
	public void checkForValidConsumerProperties(Properties props) {
		Preconditions.checkNotNull(props.get(JdbcSourceConnectorConfig.JDBC_DRIVER_NAME),"Illegal Argument passed: JDBC Driver Name is Null.");
		Preconditions.checkNotNull(props.get(JdbcSourceConnectorConfig.DB_URL),"Illegal Argument passed: JDBC DB URl is Null.");
		Preconditions.checkNotNull(props.get(JdbcSourceConnectorConfig.USERNAME),"Illegal Argument passed: JDBC Username is Null.");
		Preconditions.checkNotNull(props.get(JdbcSourceConnectorConfig.PASSWORD),"Illegal Argument passed: JDBC Password is Null.");
		Preconditions.checkNotNull(props.get(JdbcSourceConnectorConfig.QUERY_STRING),"Illegal Argument passed: JDBC Driver Sql query is Null.");
	}
}
