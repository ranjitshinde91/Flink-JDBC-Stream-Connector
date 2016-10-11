package com.gslab.com.flink.jdbc.connector.consumer;


import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.api.common.functions.RuntimeContext;

import com.gslab.com.flink.jdbc.connector.querier.AbstractQuerier;
import com.gslab.com.flink.jdbc.connector.querier.BulkTableQuerier;
import com.gslab.com.flink.jdbc.connector.querier.TimestampIncrementingTableQuerier;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;


public class JdbcConsumer<T> extends AbstractJdbcConsumer<T>{

	private static final long serialVersionUID = -286623023203258475L;

	public JdbcConsumer(DeserializationSchema<T> valueDeserializer, Properties props){
		super(valueDeserializer, props);
	}
	
	@Override
	protected AbstractQuerier<T> createQuerier(RuntimeContext runtimeContext, DeserializationSchema<T> valueDeserializer, Properties properties) throws ClassNotFoundException, SQLException {
		String mode = properties.getProperty(JdbcSourceConnectorConfig.QUERY_MODE);
		switch(mode){
			case JdbcSourceConnectorConfig.MODE_BULK:
				return new BulkTableQuerier(runtimeContext, valueDeserializer, properties);
			case JdbcSourceConnectorConfig.MODE_TIMESTAMP:
			case JdbcSourceConnectorConfig.MODE_INCREMENTING:
				return new TimestampIncrementingTableQuerier(runtimeContext, valueDeserializer, properties);
			default:
		          throw new IllegalArgumentException("Unexpected query mode: " + mode);
		}
	}
}
