package com.stark.leon.demos.connectors;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;

/**
 * @title: ElasticsearchConnector
 * @author: Leon Stark
 * @date: 2021/3/8 22:40
 * @desc:   Sink: Streaming Append Mode
 *          Sink: Streaming Upsert Mode
 *          Format: JSON-only
 * @msg:  https://blog.csdn.net/lisongjia123/article/details/81121994
 *          https://stackoverflow.com/questions/59453477/flink-elasticsearch-connector
 */
public class ElasticsearchConnector {
	public static void main(String[] args) {
		// 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 2.使用connect方式读取es数据
		tableEnv.connect(
				new Elasticsearch()
						.version("6")                      // required: valid connector versions are "6"
						.host("localhost", 9200, "http")   // required: one or more Elasticsearch hosts to connect to
						.index("MyUsers")                  // required: Elasticsearch index
						.documentType("user")              // required: Elasticsearch document type

						.keyDelimiter("$")        // optional: delimiter for composite keys ("_" by default)
						//   e.g., "$" would result in IDs "KEY1$KEY2$KEY3"
						.keyNullLiteral("n/a")    // optional: representation for null fields in keys ("null" by default)

						// optional: failure handling strategy in case a request to Elasticsearch fails (fail by default)
						.failureHandlerFail()          // optional: throws an exception if a request fails and causes a job failure
						.failureHandlerIgnore()        //   or ignores failures and drops the request
						.failureHandlerRetryRejected() //   or re-adds requests that have failed due to queue capacity saturation
						//.failureHandlerCustom(...)     //   or custom failure handling with a ActionRequestFailureHandler subclass
						// optional: configure how to buffer elements before sending them in bulk to the cluster for efficiency
						.disableFlushOnCheckpoint()    // optional: disables flushing on checkpoint (see notes below!)
						.bulkFlushMaxActions(42)       // optional: maximum number of actions to buffer for each bulk request
						.bulkFlushMaxSize("42 mb")     // optional: maximum size of buffered actions in bytes per bulk request
						//   (only MB granularity is supported)
						.bulkFlushInterval(60000L)     // optional: bulk flush interval (in milliseconds)

						.bulkFlushBackoffConstant()    // optional: use a constant backoff type
						.bulkFlushBackoffExponential() //   or use an exponential backoff type
						.bulkFlushBackoffMaxRetries(3) // optional: maximum number of retries
						.bulkFlushBackoffDelay(30000L) // optional: delay between each backoff attempt (in milliseconds)

						// optional: connection properties to be used during REST communication to Elasticsearch
						.connectionMaxRetryTimeout(3)  // optional: maximum timeout (in milliseconds) between retries
						.connectionPathPrefix("/v1")   // optional: prefix string to be added to every REST communication

		).withFormat(new Json()).createTemporaryTable("");
	}
}
