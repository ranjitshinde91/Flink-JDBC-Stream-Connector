package com.gslab.com.flink.jdbc.connector.consumer;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.gslab.com.flink.jdbc.connector.querier.BulkTableQuerier;
import com.gslab.com.flink.jdbc.connector.querier.Querier;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;


public class JdbcConsumer<T> extends AbstractJdbcConsumer<T>{

	private static final long serialVersionUID = -286623023203258475L;

	public JdbcConsumer(DeserializationSchema<T> valueDeserializer, Properties props){
		super(valueDeserializer, props);
	}
	
	@Override
	protected Querier<T> createQuerier(SourceContext<T> sourceContext, StreamingRuntimeContext runtimeContext, DeserializationSchema<T> valueDeserializer,
												Properties properties) throws ClassNotFoundException, SQLException {
		return new BulkTableQuerier(sourceContext, runtimeContext, valueDeserializer, properties);
	}

}
