package com.gslab.com.flink.jdbc.connector.example;


import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.BulkProcessorIndexer;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import com.fasterxml.jackson.databind.ObjectMapper;
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
        properties.setProperty(JdbcSourceConnectorConfig.QUERY_MODE, JdbcSourceConnectorConfig.MODE_BULK);
       
        /**properties.setProperty(JdbcSourceConnectorConfig.QUERY_MODE, JdbcSourceConnectorConfig.MODE_INCREMENTING);
        properties.setProperty(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "user_uid");**/
        
        /**properties.setProperty(JdbcSourceConnectorConfig.QUERY_MODE, JdbcSourceConnectorConfig.MODE_TIMESTAMP);
        properties.setProperty(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, "created_dt");**/
        
        DeserializationSchema<ObjectNode> jsonDeserialiationSchema =  new JSONDeserializationSchema();
        DataStreamSource<ObjectNode> stream = (DataStreamSource<ObjectNode>)env.addSource(new JdbcConsumer(jsonDeserialiationSchema, properties));
        
        stream.addSink(new Sink());
        
        
        String ipAddress = "127.0.0.1";
        int port = 9300;
        String clusterName = "my-application";
        String indexName = "user";
        String indexType = "user-type";
		
        Map<String, String> config = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put("bulk.flush.max.actions", "1");
		config.put("cluster.name", clusterName);

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName(ipAddress), port));

		/**stream.addSink(new ElasticsearchSink(config, transports, new ElasticsearchSinkFunction<ObjectNode>() {
		  ObjectMapper mapper = new ObjectMapper();
		  public IndexRequest createIndexRequest(ObjectNode element) {
		    Map<String, Map> json = new HashMap<>();
		    Map<String, Object> result = mapper.convertValue(element, Map.class);
		    json.put("data", result);

		    return Requests.indexRequest()
		            .index(indexName)
		            .type(indexType)
		            .source(json);
		  }

		  @Override
		  public void process(ObjectNode element, RuntimeContext ctx, RequestIndexer indexer) {
		    indexer.add(createIndexRequest(element));
		  }
		}));**/
        env.execute();
	}
}
