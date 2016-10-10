package com.gslab.com.flink.jdbc.connector.querier;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.gslab.com.flink.jdbc.connector.consumer.ConsumerProperties;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;

public abstract class AbstractQuerier<T> implements Querier<T>{

	protected final SourceContext<T> sourceContext;
	protected final RuntimeContext runtimeContext;
	protected final DeserializationSchema<T> deserializer;
	protected Properties consProp;
	protected transient Connection dbConn;
	protected PreparedStatement stmt;
	private static Logger LOGGER = LoggerFactory.getLogger(AbstractQuerier.class);

	public AbstractQuerier(SourceContext<T> sourceContext, StreamingRuntimeContext runtimeContext, DeserializationSchema<T> deserializer, Properties props) throws ClassNotFoundException, SQLException {
		checkForValidConsumerProperties(props);
		this.sourceContext = sourceContext;
		this.runtimeContext = runtimeContext;
		this.deserializer = deserializer;
		this.consProp = props;
		openConnection();
	}
	
	public void openConnection() throws ClassNotFoundException, SQLException{
		LOGGER.info("loading driver class.");
		Class.forName(this.consProp.getProperty(ConsumerProperties.JDBC_DRIVER_NAME));
		
		LOGGER.info("Connecting to a selected database...");
		dbConn = (Connection) DriverManager.getConnection(this.consProp.getProperty(ConsumerProperties.DB_URL), this.consProp.getProperty(ConsumerProperties.USERNAME), this.consProp.getProperty(ConsumerProperties.PASSWORD));
		LOGGER.info("Connected database successfully.");
		
		LOGGER.info("Creating statement.");
		stmt = createPreparedStatement(this.consProp.getProperty(ConsumerProperties.SQL_QUERY));
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

	protected abstract PreparedStatement createPreparedStatement(String query) throws SQLException;
	
	public void checkForValidConsumerProperties(Properties props) {
		Preconditions.checkNotNull(props.get(ConsumerProperties.JDBC_DRIVER_NAME),"Illegal Argument passed: JDBC Driver Name is Null.");
		Preconditions.checkNotNull(props.get(ConsumerProperties.DB_URL),"Illegal Argument passed: JDBC DB URl is Null.");
		Preconditions.checkNotNull(props.get(ConsumerProperties.USERNAME),"Illegal Argument passed: JDBC Username is Null.");
		Preconditions.checkNotNull(props.get(ConsumerProperties.PASSWORD),"Illegal Argument passed: JDBC Password is Null.");
		Preconditions.checkNotNull(props.get(ConsumerProperties.SQL_QUERY),"Illegal Argument passed: JDBC Driver Sql query is Null.");
	}
}
