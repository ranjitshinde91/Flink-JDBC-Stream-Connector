package com.gslab.com.flink.jdbc.connector.source;


import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.google.common.base.Preconditions;
import com.gslab.com.flink.jdbc.connector.consumer.JDBCFetcher;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;



public  abstract class AbstractJDBCSource<T> extends  RichSourceFunction<T> implements ResultTypeQueryable<T>{

	protected final DeserializationSchema<T> deserializer;
	protected final Properties properties;
	
	public AbstractJDBCSource(DeserializationSchema<T> deserializer, Properties props) {
		Preconditions.checkNotNull(deserializer, "Illegal Argument passed: valueDeserializer is Null.");
		Preconditions.checkNotNull(props, "Illegal Argument passed: JDBC consumer properties are Null.");
		this.deserializer = deserializer;
		this.properties = props;
	}
	
	protected abstract JDBCFetcher<T> createFetcher(SourceContext<T> sourceContext, StreamingRuntimeContext runtimeContext, DeserializationSchema<T> valueDeserializer, Properties properties)
			throws ClassNotFoundException, SQLException;

	public void run(SourceContext<T> ctx)throws Exception {
		JDBCFetcher<T> fetcher = createFetcher(ctx, (StreamingRuntimeContext) getRuntimeContext(),  deserializer, properties);
		fetcher.runFetchLoop();
	}
		
	public void cancel() {
		
	}
	
	public TypeInformation<T> getProducedType() {
		return this.deserializer.getProducedType();
	}
	

}
