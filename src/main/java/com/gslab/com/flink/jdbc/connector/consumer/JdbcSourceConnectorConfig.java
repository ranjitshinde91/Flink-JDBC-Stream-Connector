package com.gslab.com.flink.jdbc.connector.consumer;

public class JdbcSourceConnectorConfig {
	
	public static final String JDBC_DRIVER_NAME = "drivername";
	public static final String DB_URL = "dbUrl";
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String QUERY_STRING= "query.string";
	
	public static final String QUERY_MODE = "query.mode";
	public static final String MODE_UNSPECIFIED = "";
	public static final String MODE_BULK = "bulk";
	public static final String MODE_TIMESTAMP = "timestamp";
	public static final String MODE_INCREMENTING = "incrementing";
	
	public static final String INCREMENTING_COLUMN_NAME_CONFIG = "incrementing.column.name";
	  private static final String INCREMENTING_COLUMN_NAME_DOC =
	      "The name of the strictly incrementing column to use to detect new rows. Any empty value "
	      + "indicates the column should be autodetected by looking for an auto-incrementing column. "
	      + "This column may not be nullable.";
	  public static final String INCREMENTING_COLUMN_NAME_DEFAULT = "";
	  private static final String INCREMENTING_COLUMN_NAME_DISPLAY = "Incrementing Column Name";

	  public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
	  private static final String TIMESTAMP_COLUMN_NAME_DOC =
	      "The name of the timestamp column to use to detect new or modified rows. This column may "
	      + "not be nullable.";
	  public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "";
	  private static final String TIMESTAMP_COLUMN_NAME_DISPLAY = "Timestamp Column Name";

}
