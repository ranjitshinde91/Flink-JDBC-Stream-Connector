package com.gslab.com.flink.jdbc.connector.example;


import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gslab.com.flink.jdbc.connector.consumer.JdbcSourceConnectorConfig;
import com.gslab.com.flink.jdbc.connector.consumer.JdbcConsumer;
import com.gslab.com.flink.jdbc.connector.serialization.DeserializationSchema;
import com.gslab.com.flink.jdbc.connector.serialization.JSONDeserializationSchema;

public class App {
	public static void main(String[] args) throws Exception {
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        
        Properties properties = new Properties();
        properties.setProperty(JdbcSourceConnectorConfig.JDBC_DRIVER_NAME, "com.mysql.jdbc.Driver");
        properties.setProperty(JdbcSourceConnectorConfig.DB_URL, "jdbc:mysql://localhost/kzphase1");
        properties.setProperty(JdbcSourceConnectorConfig.USERNAME, "root");
        properties.setProperty(JdbcSourceConnectorConfig.PASSWORD, "123");
        properties.setProperty(JdbcSourceConnectorConfig.QUERY_STRING, "SELECT * FROM user");
       // properties.setProperty(JdbcSourceConnectorConfig.QUERY_MODE, JdbcSourceConnectorConfig.MODE_BULK);
       
        /**properties.setProperty(JdbcSourceConnectorConfig.QUERY_MODE, JdbcSourceConnectorConfig.MODE_INCREMENTING);
        properties.setProperty(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "user_uid");**/
        
        properties.setProperty(JdbcSourceConnectorConfig.QUERY_MODE, JdbcSourceConnectorConfig.MODE_TIMESTAMP);
        properties.setProperty(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, "created_dt");
        
        DeserializationSchema<ObjectNode> jsonDeserialiationSchema =  new JSONDeserializationSchema();
        DataStreamSource<ObjectNode> stream = (DataStreamSource<ObjectNode>)env.addSource(new JdbcConsumer(jsonDeserialiationSchema, properties));
        
		
        stream.print();
        
        env.execute();
	}

}
