package com.gslab.com.flink.jdbc.connector.source;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.gslab.com.flink.jdbc.connector.consumer.JDBCFetcher;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;


public class JDBCSource<T> extends AbstractJDBCSource<T>{
	

	public JDBCSource(DeserializationSchema<T> valueDeserializer, Properties props){
		super(valueDeserializer, props);
	}
	
	@Override
	protected JDBCFetcher<T> createFetcher(SourceContext<T> sourceContext, StreamingRuntimeContext runtimeContext, DeserializationSchema<T> valueDeserializer,  Properties properties) 
			throws ClassNotFoundException, SQLException {
		return new JDBCFetcher(sourceContext, runtimeContext, valueDeserializer, properties);
	}



}
