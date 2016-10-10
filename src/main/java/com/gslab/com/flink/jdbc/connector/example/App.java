package com.gslab.com.flink.jdbc.connector.example;


import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gslab.com.flink.jdbc.connector.consumer.ConsumerProperties;
import com.gslab.com.flink.jdbc.connector.consumer.JdbcConsumer;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;
import com.gslab.com.flink.jdbc.connector.serialization.JSONDeserializationSchema;

public class App {
	public static void main(String[] args) throws Exception {
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        
        Properties properties = new Properties();
        properties.setProperty(ConsumerProperties.JDBC_DRIVER_NAME, "com.mysql.jdbc.Driver");
        properties.setProperty(ConsumerProperties.DB_URL, "jdbc:mysql://localhost/kzphase1");
        properties.setProperty(ConsumerProperties.USERNAME, "root");
        properties.setProperty(ConsumerProperties.PASSWORD, "123");
        properties.setProperty(ConsumerProperties.SQL_QUERY, "SELECT * FROM user");
        
        DeserializationSchema<ObjectNode> jsonDeserialiationSchema =  new JSONDeserializationSchema();
        DataStreamSource<ObjectNode> stream = (DataStreamSource<ObjectNode>)env.addSource(new JdbcConsumer(jsonDeserialiationSchema, properties));
		
        stream.print();
        
        env.execute();
	}

}
