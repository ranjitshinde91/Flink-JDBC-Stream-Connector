package com.gslab.com.flink.jdbc.connector.querier;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.TimeZone;

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
	  
	  public TimestampIncrementingTableQuerier(DeserializationSchema<T> deserializer, Properties props) throws ClassNotFoundException, SQLException {
		  super(deserializer, props);
		  checkForValidTimeOrIncProperties(props);
		  this.incrementingColumn = props.getProperty(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, null);
		  this.timestampColumn = props.getProperty(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, null);
		  timestampOffset = 0L;
		  incrementingOffset = 0L;
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

	  public void fetchAndEmitRecords(SourceContext<T> sourceContext) {
		  this.sourceContext = sourceContext;
			try {
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
				LOGGER.info("fetching records from database.");
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					T value = deserializer.deserialize(rs);
					emitRecord(value);
				}
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally{
				closeConnection();
			}
			
	  }
	  protected void emitRecord(T record) {
			sourceContext.collect(record);
	  }
	  
	  public void checkForValidTimeOrIncProperties(Properties props) {
		  
	  }
}
