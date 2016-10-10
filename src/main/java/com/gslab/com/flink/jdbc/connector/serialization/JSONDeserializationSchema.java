package com.gslab.com.flink.jdbc.connector.serialization;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mysql.jdbc.ResultSetMetaData;


public class JSONDeserializationSchema extends AbstractDeserializationSchema<ObjectNode>{
	
	private ObjectMapper mapper;
	private static final Calendar UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
	
	public JSONDeserializationSchema(){
		mapper = new ObjectMapper();
	}
	
	public ObjectNode deserialize(ResultSet message) throws SQLException {
		ObjectNode objectNode = mapper.createObjectNode();
		ResultSetMetaData rsmd = (ResultSetMetaData) message.getMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int index = 1; index <= columnCount; index++) {
            String column = rsmd.getColumnName(index);
            int sqlType = rsmd.getColumnType(index);
            switch (sqlType) {
            case Types.INTEGER:
                objectNode.put(column, message.getInt(index));
                break;
            case Types.BIGINT:
                objectNode.put(column, message.getLong(index));
                break;
               
            case Types.REAL: 
                objectNode.put(column, message.getFloat(index));
                break;
              case Types.FLOAT:
              case Types.DOUBLE: 
                objectNode.put(column, message.getDouble(index));
                break;
              case Types.NUMERIC:
              case Types.DECIMAL: {
                objectNode.put(column, message.getBigDecimal(index));
                break;
              }

              case Types.CHAR:
              case Types.VARCHAR:
              case Types.LONGVARCHAR: {
                objectNode.put(column, message.getString(index));
                break;
              }

              case Types.NCHAR:
              case Types.NVARCHAR:
              case Types.LONGNVARCHAR: {
                objectNode.put(column, message.getNString(index));
                break;
              }

              // Binary == fixed, VARBINARY and LONGVARBINARY == bytes
              case Types.BINARY:
              case Types.VARBINARY:
              case Types.LONGVARBINARY: {
                objectNode.put(column, message.getBytes(index));
                break;
              }

              // Date is day + moth + year
              case Types.DATE: {
            	  if(message.getDate(index, UTC_CALENDAR) !=null){
              		  objectNode.put(column, message.getDate(index, UTC_CALENDAR).getTime());
              	  }
                break;
              }

              // Time is a time of day -- hour, minute, seconds, nanoseconds
              case Types.TIME: {
                if(message.getTime(index, UTC_CALENDAR) !=null){
          		  objectNode.put(column, message.getTime(index, UTC_CALENDAR).getTime());
          	  	}
                break;
              }

              case Types.TIMESTAMP: {
            	  if(message.getTimestamp(index, UTC_CALENDAR) != null){
            		  objectNode.put(column, message.getTimestamp(index, UTC_CALENDAR).getTime());
            	  }
                break;
              }
            }
        }
	return objectNode;
	}
}

