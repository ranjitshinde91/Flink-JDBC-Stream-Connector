package com.gslab.com.flink.jdbc.connector.serialization;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;


public abstract class AbstractDeserializationSchema <T> implements DeserializationSchema<T>{

	public TypeInformation<T> getProducedType() {
		return TypeExtractor.createTypeInfo(AbstractDeserializationSchema.class, getClass(), 0, null, null);
	}


	public boolean isEndOfStream(T nextElement) {
		return false;
	}

}
