package com.gslab.com.flink.jdbc.connector.querier;

import java.io.Serializable;
import java.net.ConnectException;
import java.sql.SQLException;

import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;


public interface Querier<T> extends Checkpointed<Serializable>{
	
	void openConnection() throws ClassNotFoundException, SQLException;
	
	void fetchAndEmitRecords(SourceContext<T> sourceContext) throws SQLException, ConnectException;

	void closeConnection();
}
