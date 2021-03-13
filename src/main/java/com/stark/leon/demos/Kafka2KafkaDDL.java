package com.stark.leon.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title: Kafka2KafkaDDL
 * @author: Leon Stark
 * @date: 2021/3/13 22:36
 * @desc:
 * @msg:    write data from kafka source into kafka sink
 */
public class Kafka2KafkaDDL {
	public static void main(String[] args) throws Exception {
		// 1. 获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 2. 创建表执行环境
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 3. source
		String sourceSql = "create table source_sensor (" +
				"id string, ts bigint, vc int" +
				") with (" +
				"   'connector'='kafka'," +
				"   'topic'='test_source'," +
				"   'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092'," +
				"   'properties.group.id' = 'testGroup'," +
				"   'format' = 'json'" +
				"   )";
		tableEnv.executeSql(sourceSql);

		// 4. sink
		String sinkSql = "create table sink_sensor (" +
				"id string, ts bigint, vc int" +
				") with (" +
				"   'connector'='kafka'," +
				"   'topic'='test_sink'," +
				"   'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092'," +
				"   'scan.startup.mode' = 'earliest-offset'," +
				"   'format' = 'json'" +
				"   )";
		tableEnv.executeSql(sinkSql);

		// 5. 将查询结果写入结果表
		tableEnv.executeSql("insert into sink_sensor select * from source_sensor where vc > 45");

	}
}
