package com.stark.leon.demos.time.attr;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @title: ProcessTimeAttribute
 * @author: Leon Stark
 * @date: 2021/3/24 20:46
 * @desc:
 *          声明DDL 时指定 事件时间 字段 -- 事件时间
 *          https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/streaming/time_attributes.html
 * @msg:
 */
public class EventTimeAttributeOnDDL {
	public static void main(String[] args) {
		// 1. 获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 2. 使用DDL方式指定事件时间字段
		TableResult result = tableEnv.executeSql("create table sensor (" +
				"id string," +
				"ts bigint," +
				"vc int," +
				"rt as to_timestamp(from_unixtimep(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
				"watermark for rt as rt - interval '5' second" +
				") with(" +
				"   'connector' = 'kafka'," +
				"   'topic'='test_source'," +
				"   'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092'," +
				"   'properties.group.id' = 'testGroup'," +
				"   'format' = 'json'" +
				"   )"
		);

		// 声明处理时间
		Table sensor = tableEnv.from("sensor");

		sensor.printSchema();
		sensor.execute().print();
	}
}
