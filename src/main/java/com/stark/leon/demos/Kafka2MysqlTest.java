package com.stark.leon.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title: Kafka2MysqlTest
 * @author: Leon Stark
 * @date: 2021/3/13 23:07
 * @desc:
 * @msg:
 */
public class Kafka2MysqlTest {
	public static void main(String[] args) {
		// 1. 获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 2. 创建表执行环境
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 3. Kafka Source
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

		// 4. MySQL Sink (程序不会自动在mysql中建表，也就是说只能声明mysql中已经存在的表)
		String sinkSql = "create table sink_sensor (" +
				"id string, ts bigint, vc int, primary key (id) not enforced" +
				") with (" +
				"   'connector'='jdbc'," +
				"    'url' = 'jdbc:mysql://localhost:3306/mydatabase'," +
				"   'table-name' = 'sink_users'" +
				"   'username'='root'" +
				"   'password'='123456'" +
				"   'sink.buffer-flush.max-rows'='500'" +
				"   'sink.buffer-flush.interval='2s'" +
				"   )";
		tableEnv.executeSql(sinkSql);

		// 5. 将查询结果写入结果表
		tableEnv.executeSql("insert into sink_sensor select * from source_sensor where vc > 45");

	}
}
