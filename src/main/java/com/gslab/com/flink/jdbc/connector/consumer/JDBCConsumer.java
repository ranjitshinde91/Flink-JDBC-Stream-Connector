package com.gslab.com.flink.jdbc.connector.consumer;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;


public class JDBCConsumer<T> extends AbstractJDBCConsumer<T>{
	

	public JDBCConsumer(DeserializationSchema<T> valueDeserializer, Properties props){
		super(valueDeserializer, props);
	}
	
	@Override
	protected JDBCFetcher<T> createFetcher(SourceContext<T> sourceContext, StreamingRuntimeContext runtimeContext, DeserializationSchema<T> valueDeserializer,  Properties properties) 
			throws ClassNotFoundException, SQLException {
		return new JDBCFetcher(sourceContext, runtimeContext, valueDeserializer, properties);
	}



}
