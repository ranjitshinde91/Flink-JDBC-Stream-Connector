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
import com.gslab.com.flink.jdbc.connector.util.JdbcUtils;

public class BulkTableQuerier<T> extends AbstractQuerier<T>{
	private static Logger LOGGER = LoggerFactory.getLogger(BulkTableQuerier.class);
	
	private volatile Long numElementsEmitted = 0L;
	private boolean isRSExhausted = false;;

	public BulkTableQuerier(RuntimeContext runtimeContext, DeserializationSchema<T> deserializer, Properties props) throws ClassNotFoundException, SQLException {
		super(runtimeContext, deserializer, props);
	}

	public void fetchAndEmitRecords(SourceContext<T> sourceContext) throws SQLException{
		this.sourceContext = sourceContext;
		while(isRunning && !isRSExhausted){
			LOGGER.info("fetching records from database from "+this.numElementsEmitted+" to "+(this.numElementsEmitted+this.fetchSize));
			stmt.setLong(1, this.numElementsEmitted);
			stmt.setLong(2, this.numElementsEmitted+this.fetchSize);
			ResultSet rs = stmt.executeQuery();
			this.checkpointLock = sourceContext.getCheckpointLock();
			if (!rs.next()) {                            //if rs.next() returns false
				isRSExhausted = true;
			}
			else {
				do {
					T value = deserializer.deserialize(rs);
					emitRecord(value);
				}while (rs.next());
			}
			rs.close();
		}
	}


	@Override
	protected PreparedStatement createPreparedStatement()throws SQLException {
		 StringBuilder builder = new StringBuilder();
		 builder.append(this.consProp.getProperty(JdbcSourceConnectorConfig.QUERY_STRING));
		 builder.append(" limit  ?,?");
		 String queryString = builder.toString();
		 LOGGER.debug("{} prepared SQL query: {}", this, queryString);
		 stmt = dbConn.prepareStatement(queryString);
		 return stmt;
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
		this.numElementsEmitted = (Long) Long.parseLong(state.toString());
	}

}
