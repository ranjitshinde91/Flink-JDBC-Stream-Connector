package com.gslab.com.flink.jdbc.connector.querier;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;

public class BulkTableQuerier<T> extends AbstractQuerier<T>{
	private static Logger LOGGER = LoggerFactory.getLogger(BulkTableQuerier.class);

	public BulkTableQuerier(DeserializationSchema<T> deserializer, Properties props) throws ClassNotFoundException, SQLException {
		super(deserializer, props);
	}

	public void fetchAndEmitRecords(SourceContext<T> sourceContext){
		this.sourceContext = sourceContext;
		try {
			LOGGER.info("fetching records from database.");
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				T value = deserializer.deserialize(rs);
				emitRecord(value);
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			closeConnection();
		}
	}


	@Override
	protected PreparedStatement createPreparedStatement(String query)throws SQLException {
		return dbConn.prepareStatement(query);
	}
	
	protected void emitRecord(T record) {
		sourceContext.collect(record);
	}

}
