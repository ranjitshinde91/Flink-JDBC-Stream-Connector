package com.gslab.com.flink.jdbc.connector.serialization;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public interface DeserializationSchema<T> extends Serializable, ResultTypeQueryable<T>{
	
	T deserialize(ResultSet message) throws SQLException;

	boolean isEndOfStream(T nextElement);
}
