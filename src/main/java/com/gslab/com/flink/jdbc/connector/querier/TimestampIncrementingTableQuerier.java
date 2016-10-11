package com.gslab.com.flink.jdbc.connector.querier;

import java.io.Serializable;
import java.net.ConnectException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gslab.com.flink.jdbc.connector.consumer.JdbcSourceConnectorConfig;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;
import com.gslab.com.flink.jdbc.connector.util.JdbcUtils;

public class TimestampIncrementingTableQuerier<T> extends AbstractQuerier<T>{
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(TimestampIncrementingTableQuerier.class);

	  private static final Calendar UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

	  private static final long DEFAULT_TIMESTAMP_DELAY = 30;
	  private String timestampColumn;
	  private Long timestampOffset;
	  private String incrementingColumn;
	  private Long incrementingOffset;
	  private long timestampDelay = DEFAULT_TIMESTAMP_DELAY;
	  protected transient JdbcConsumerState jdbcConsumerState;
	  
	  public TimestampIncrementingTableQuerier(RuntimeContext runtimeContext, DeserializationSchema<T> deserializer, Properties props) throws ClassNotFoundException, SQLException {
		  super(runtimeContext, deserializer, props);
		  checkForValidTimeOrIncProperties(props);
		  this.incrementingColumn = props.getProperty(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, null);
		  this.timestampColumn = props.getProperty(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, null);
		  timestampOffset = 0L;
		  incrementingOffset = 0L;
		  jdbcConsumerState = new JdbcConsumerState();
	  }

	  protected PreparedStatement createPreparedStatement()throws SQLException {
		 StringBuilder builder = new StringBuilder();
		 builder.append(this.consProp.getProperty(JdbcSourceConnectorConfig.QUERY_STRING));
		 String quoteString = JdbcUtils.getIdentifierQuoteString(dbConn); 
		 if (incrementingColumn != null) {
		      builder.append(" WHERE ");
		      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
		      builder.append(" > ?");
		      builder.append(" ORDER BY ");
		      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
		      builder.append(" ASC");
		    } else if (timestampColumn != null) {
		      builder.append(" WHERE ");
		      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
		      builder.append(" > ? AND ");
		      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
		      builder.append(" < ? ORDER BY ");
		      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
		      builder.append(" ASC");
		    }
		    String queryString = builder.toString();
		    LOGGER.debug("{} prepared SQL query: {}", this, queryString);
		    stmt = dbConn.prepareStatement(queryString);
			return stmt;
	  }

	  public void fetchAndEmitRecords(SourceContext<T> sourceContext) throws SQLException, ConnectException {
		  this.sourceContext = sourceContext;
		  
		  while(isRunning){
				if (incrementingColumn != null) {
				      stmt.setLong(1, (incrementingOffset == null ? -1 : incrementingOffset));
				      LOGGER.debug("Executing prepared statement with incrementing value = " + incrementingOffset);
				    } else if (timestampColumn != null) {
				      Timestamp startTime = new Timestamp(timestampOffset == null ? 0 : timestampOffset);
				      Timestamp endTime = new Timestamp(JdbcUtils.getCurrentTimeOnDB(stmt.getConnection(), UTC_CALENDAR).getTime() - timestampDelay);
				      stmt.setTimestamp(1, startTime, UTC_CALENDAR);
				      stmt.setTimestamp(2, endTime, UTC_CALENDAR);
				      LOGGER.debug("Executing prepared statement with timestamp value = " + timestampOffset + " (" + JdbcUtils.formatUTC(startTime) + ") " + " end time " + JdbcUtils.formatUTC(endTime));
				    }
				ResultSet rs = stmt.executeQuery();
				this.checkpointLock = sourceContext.getCheckpointLock();
				while (rs.next()) {
					T value = deserializer.deserialize(rs);
					emitRecord(value);
					if(deserializer.isEndOfStream(rs)){
						updateOffset(rs);
					}
				}
				rs.close();
				isRunning = false;
		  }
	  }
	  
	  private void updateOffset(ResultSet rs) throws SQLException {
		  if (incrementingColumn != null) {
			  this.incrementingOffset = rs.getLong(this.incrementingColumn);
		  }
		  else if(timestampColumn != null){
			  this.timestampOffset = rs.getLong(this.timestampColumn);
		  }
	  }

	protected void emitRecord(T record) {
			sourceContext.collect(record);
	}
	  
	public void checkForValidTimeOrIncProperties(Properties props) {
		  
	}
	  

	@Override
	public JdbcConsumerState snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		this.jdbcConsumerState.setIncrementingOffset(this.incrementingOffset);
		this.jdbcConsumerState.setTimestampOffset(this.timestampOffset);
		
		System.out.println("this.timestampOffset "+this.timestampOffset +" this.incrementingOffset "+this.incrementingOffset);
		return this.jdbcConsumerState;
	}

	@Override
	public void restoreState(Serializable state) throws Exception {
		this.jdbcConsumerState = (JdbcConsumerState)state;
	}
	
	
	private static class JdbcConsumerState implements Serializable{

		private static final long serialVersionUID = -2658018473366265467L;
		private Long timestampOffset;
		private Long incrementingOffset;

		public JdbcConsumerState(Long timestampOffset, Long incrementingOffset) {
			this.timestampOffset = timestampOffset;
			this.incrementingOffset = incrementingOffset;
		}

		public Long getTimestampOffset() {
			return timestampOffset;
		}

		public void setTimestampOffset(Long timestampOffset) {
			this.timestampOffset = timestampOffset;
		}

		public Long getIncrementingOffset() {
			return incrementingOffset;
		}

		public void setIncrementingOffset(Long incrementingOffset) {
			this.incrementingOffset = incrementingOffset;
		}

		public JdbcConsumerState() {
			this.timestampOffset = 0L;
			this.incrementingOffset = 0L;
		}
	}
}
