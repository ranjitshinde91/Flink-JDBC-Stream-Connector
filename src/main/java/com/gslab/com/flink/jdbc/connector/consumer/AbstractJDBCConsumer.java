package com.gslab.com.flink.jdbc.connector.consumer;


import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.google.common.base.Preconditions;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;



public  abstract class AbstractJDBCConsumer<T> extends  RichSourceFunction<T> implements ResultTypeQueryable<T>, CheckpointListener, CheckpointedAsynchronously<Long>{

	public Long snapshotState(long checkpointId, long checkpointTimestamp)throws Exception {
		System.out.println("snapshotState");
		return 123l;
	}

	public void restoreState(Long state) throws Exception {
		System.out.println("restore state");
	}

	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		System.out.println("notifyCheckpointComplete");
		
	}

	protected final DeserializationSchema<T> deserializer;
	protected final Properties properties;
	
	public AbstractJDBCConsumer(DeserializationSchema<T> deserializer, Properties props) {
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
