package com.gslab.com.flink.jdbc.connector.querier;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gslab.com.flink.jdbc.connector.consumer.JdbcSourceConnectorConfig;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;

public class BulkTableQuerier<T> extends AbstractQuerier<T>{
	private static Logger LOGGER = LoggerFactory.getLogger(BulkTableQuerier.class);
	
	private volatile Long numElementsEmitted = 0L;

	private volatile Long numElementsToSkip = 0L;

	private boolean isRSExhausted = false;;

	public BulkTableQuerier(RuntimeContext runtimeContext, DeserializationSchema<T> deserializer, Properties props) throws ClassNotFoundException, SQLException {
		super(runtimeContext, deserializer, props);
	}

	public void fetchAndEmitRecords(SourceContext<T> sourceContext) throws SQLException{
		this.sourceContext = sourceContext;
		while(isRunning && !isRSExhausted){
			LOGGER.info("fetching records from database.");
			ResultSet rs = stmt.executeQuery();
			
			for(Long i=0L;i<=this.numElementsToSkip;i++){
				rs.next();
			}
			checkpointLock = sourceContext.getCheckpointLock();
			while (rs.next()) {
				T value = deserializer.deserialize(rs);
				emitRecord(value);
			}
			rs.close();
			isRSExhausted = true;
		}
	}


	@Override
	protected PreparedStatement createPreparedStatement()throws SQLException {
		String query = this.consProp.getProperty(JdbcSourceConnectorConfig.QUERY_STRING);
		return dbConn.prepareStatement(query);
	}
	
	protected void emitRecord(T record) {
		synchronized (checkpointLock) {
			sourceContext.collect(record);
			numElementsEmitted++;
		}
	}

	@Override
	public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		return this.numElementsEmitted;
	}

	@Override
	public void restoreState(Serializable state) throws Exception {
		this.numElementsToSkip = (Long) Long.parseLong(state.toString());
	}

}
