package com.gslab.com.flink.jdbc.connector.util;

import java.net.ConnectException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JdbcUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcUtils.class);
	
	 private static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER = new ThreadLocal<SimpleDateFormat>() {
		    @Override
		    protected SimpleDateFormat initialValue() {
		      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		      return sdf;
		    }
     };
	
	/**
	   * Get the string used for quoting identifiers in this database's SQL dialect.
	   * @param connection the database connection
	   * @return the quote string
	   * @throws SQLException
	   */
	  public static String getIdentifierQuoteString(Connection connection) throws SQLException {
	    String quoteString = connection.getMetaData().getIdentifierQuoteString();
	    quoteString = quoteString == null ? "" : quoteString;
	    return quoteString;
	  }
	  
	  
	  /**
	   * Quote the given string.
	   * @param orig the string to quote
	   * @param quote the quote character
	   * @return the quoted string
	   */
	  public static String quoteString(String orig, String quote) {
	    return quote + orig + quote;
	  }
	  
	  
	  /**
	   * Return current time at the database
	   * @param conn
	   * @param cal
	   * @return
	   */
	  public static Timestamp getCurrentTimeOnDB(Connection conn, Calendar cal) throws SQLException, ConnectException {
	    String query;

	    // This is ugly, but to run a function, everyone does 'select function()'
	    // except Oracle that does 'select function() from dual'
	    // and Derby uses either the dummy table SYSIBM.SYSDUMMY1  or values expression (I chose to use values)
	    String dbProduct = conn.getMetaData().getDatabaseProductName();
	    if ("Oracle".equals(dbProduct))
	      query = "select CURRENT_TIMESTAMP from dual";
	    else if ("Apache Derby".equals(dbProduct))
	      query = "values(CURRENT_TIMESTAMP)";
	    else
	      query = "select CURRENT_TIMESTAMP;";

	    try (Statement stmt = conn.createStatement()) {
	      LOGGER.debug("executing query " + query + " to get current time from database");
	      ResultSet rs = stmt.executeQuery(query);
	      if (rs.next())
	        return rs.getTimestamp(1, cal);
	      else
	        throw new ConnectException("Unable to get current time from DB using query " + query + " on database " + dbProduct);
	    } catch (SQLException e) {
	      LOGGER.error("Failed to get current time from DB using query " + query + " on database " + dbProduct, e);
	      throw e;
	    }
	  }
	  
	  public static String formatUTC(Date date) {
		    return DATE_FORMATTER.get().format(date);
	  }


}
