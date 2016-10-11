package com.gslab.com.flink.jdbc.connector.serialization;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;


public abstract class AbstractDeserializationSchema <T> implements DeserializationSchema<T>{
	private static final long serialVersionUID = -5286287743504673260L;

	public TypeInformation<T> getProducedType() {
		return TypeExtractor.createTypeInfo(AbstractDeserializationSchema.class, getClass(), 0, null, null);
	}

	public boolean isEndOfStream(ResultSet nextElement) throws SQLException {
		return nextElement.isLast();
	}
}
