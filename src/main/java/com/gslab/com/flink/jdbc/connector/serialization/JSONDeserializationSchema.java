package com.gslab.com.flink.jdbc.connector.serialization;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mysql.jdbc.ResultSetMetaData;


public class JSONDeserializationSchema extends AbstractDeserializationSchema<ObjectNode>{
	
	private ObjectMapper mapper;
	
	public JSONDeserializationSchema(){
		mapper = new ObjectMapper();
	}
	
	public ObjectNode deserialize(ResultSet message) throws SQLException {
		ObjectNode objectNode = mapper.createObjectNode();
		ResultSetMetaData rsmd = (ResultSetMetaData) message.getMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int index = 1; index <= columnCount; index++) {
            String column = rsmd.getColumnName(index);
            Object value = message.getObject(index);
            if (value == null) {
                objectNode.putNull(column);
            } else if (value instanceof Integer) {
                objectNode.put(column, (Integer) value);
            } else if (value instanceof String) {
                objectNode.put(column, (String) value);                
            } else if (value instanceof Boolean) {
                objectNode.put(column, (Boolean) value);           
            } else if (value instanceof Date) {
                objectNode.put(column, ((Date) value).getTime());                
            } else if (value instanceof Long) {
                objectNode.put(column, (Long) value);                
            } else if (value instanceof Double) {
                objectNode.put(column, (Double) value);                
            } else if (value instanceof Float) {
                objectNode.put(column, (Float) value);                
            } else if (value instanceof BigDecimal) {
                objectNode.put(column, (BigDecimal) value);
            } else if (value instanceof Byte) {
                objectNode.put(column, (Byte) value);
            } else if (value instanceof byte[]) {
                objectNode.put(column, (byte[]) value);                
            } else {
          	 // objectNode.put(column,  value);   
               // throw new IllegalArgumentException("Unmappable object type: " + value.getClass());
            }
	}
	return objectNode;
}

}	
