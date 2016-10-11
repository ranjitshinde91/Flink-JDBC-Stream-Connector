package com.gslab.com.flink.jdbc.connector.consumer;


import java.io.Serializable;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.google.common.base.Preconditions;
import com.gslab.com.flink.jdbc.connector.querier.AbstractQuerier;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;


public abstract class AbstractJdbcConsumer<T> extends  RichSourceFunction<T> implements ResultTypeQueryable<T>, CheckpointListener, Checkpointed<Serializable>{

	private static final long serialVersionUID = -8819974877828329443L;
	protected AbstractQuerier<T> querier;
	protected final DeserializationSchema<T> deserializer;
	protected final Properties properties;
	protected String queryMode;
	
	public AbstractJdbcConsumer (DeserializationSchema<T> deserializer, Properties props){
		Preconditions.checkNotNull(deserializer, "Illegal Argument passed: valueDeserializer is Null.");
		Preconditions.checkNotNull(props, "Illegal Argument passed: JDBC consumer properties are Null.");
		this.deserializer = deserializer;
		this.properties = props;
	}
	
	protected abstract AbstractQuerier<T> createQuerier(RuntimeContext runtimeContext, DeserializationSchema<T> valueDeserializer, Properties properties) throws ClassNotFoundException, SQLException;

	@Override
	public void run(SourceContext<T> ctx)throws Exception {
		this.querier.fetchAndEmitRecords(ctx);
	}
		

	public void createQuerier() throws Exception{
		if(this.querier == null)
		this.querier = createQuerier((StreamingRuntimeContext) getRuntimeContext(), deserializer, properties);
	}
	@Override
	public void open(Configuration parameters) throws Exception {
		createQuerier();
		this.querier.openConnection();
	}

	@Override
	public void close() throws Exception {
		this.querier.closeConnection();
	}

	
	@Override
	public void cancel() {
		this.querier.cancel();
		
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return this.deserializer.getProducedType();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		
	}
	
	@Override
	public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		return this.querier.snapshotState(checkpointId, checkpointTimestamp);
	}

	@Override
	public void restoreState(Serializable state) throws Exception {
		createQuerier();
		this.querier.restoreState(state);
	}
}
