package com.gslab.com.flink.jdbc.connector.querier;

import java.sql.SQLException;

public interface Querier<T> {
	
	void openConnection() throws ClassNotFoundException, SQLException;
	
	void fetchAndEmitRecords();

	void closeConnection();

}
