package com.gslab.com.flink.jdbc.connector.querier;

import java.sql.SQLException;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

public interface Querier<T> {
	
	void openConnection() throws ClassNotFoundException, SQLException;
	
	void fetchAndEmitRecords(SourceContext<T> sourceContext);

	void closeConnection();

}
