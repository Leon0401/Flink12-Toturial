package com.stark.leon.demos.connectors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @title: KafkaConnector
 * @author: Leon Stark
 * @date: 2021/3/8 22:22
 * @desc:  table api中使用kafka connector
 * @msg: 由于1.12 api的变化，这里使用1.10 api用于测试
 */
public class KafkaConnector {
	public static void main(String[] args) throws Exception {
		// 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/connect.html#file-system-connector
		tableEnv.connect(
				new Kafka()
						.version("universal")
						.topic("test")
						.startFromLatest()
						.property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dev-node01:9092,dev-node02:9092,dev-node03:9092")
						.property(ConsumerConfig.GROUP_ID_CONFIG, "sensor_test")
			)
				.withFormat(new Json())
				.withSchema(
						new Schema()
								.field("id", DataTypes.STRING())
								.field("ts", DataTypes.BIGINT())
								.field("vc", DataTypes.INT()))
				.createTemporaryTable("sensor_test");

		// 3.将连接器应用，转化为表
		Table sensor = tableEnv.from("sensor_test");

		// 4.查询、过滤
		Table selectTable = sensor.groupBy($("id"))
				.select($("id"), $("id").count().as("cnt"));

		// 5.转换成流 输出
		DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(selectTable, Row.class);
		tuple2DataStream.print();

		// 执行任务
		env.execute();
	}
}
