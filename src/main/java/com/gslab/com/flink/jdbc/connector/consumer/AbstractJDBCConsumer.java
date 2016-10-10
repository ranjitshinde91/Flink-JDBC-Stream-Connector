package com.gslab.com.flink.jdbc.connector.consumer;


import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.google.common.base.Preconditions;
import com.gslab.com.flink.jdbc.connector.querier.Querier;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;



public abstract class AbstractJdbcConsumer<T> extends  RichSourceFunction<T> implements ResultTypeQueryable<T>, CheckpointListener, CheckpointedAsynchronously<Long>{

	private static final long serialVersionUID = -8819974877828329443L;
	protected Querier<T> querier;
	protected final DeserializationSchema<T> deserializer;
	protected final Properties properties;
	
	public AbstractJdbcConsumer(DeserializationSchema<T> deserializer, Properties props) {
		Preconditions.checkNotNull(deserializer, "Illegal Argument passed: valueDeserializer is Null.");
		Preconditions.checkNotNull(props, "Illegal Argument passed: JDBC consumer properties are Null.");
		this.deserializer = deserializer;
		this.properties = props;
	}
	
	protected abstract Querier<T> createQuerier(SourceContext<T> sourceContext, StreamingRuntimeContext runtimeContext, DeserializationSchema<T> valueDeserializer,
			Properties properties) throws ClassNotFoundException, SQLException;

	public void run(SourceContext<T> ctx)throws Exception {
		this.querier = createQuerier(ctx, (StreamingRuntimeContext) getRuntimeContext(),  deserializer, properties);
		this.querier.fetchAndEmitRecords();
	}
		

	@Override
	public void close() throws Exception {
		this.querier.closeConnection();
	}

	public void cancel() {
		
	}
	
	public TypeInformation<T> getProducedType() {
		return this.deserializer.getProducedType();
	}
	
	public Long snapshotState(long checkpointId, long checkpointTimestamp)throws Exception {
		return 123l;
	}

	public void restoreState(Long state) throws Exception {
	}

	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		
	}
	

}
