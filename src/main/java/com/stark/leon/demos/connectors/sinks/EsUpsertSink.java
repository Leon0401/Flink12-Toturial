package com.stark.leon.demos.connectors.sinks;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @title: FileSink
 * @author: Leon Stark
 * @date: 2021/3/10 10:36
 * @desc:
 * @msg:
 */
public class EsUpsertSink {
	public static void main(String[] args) throws Exception {
		// 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2.读取端口数据创建刘并转换为Java Bean
		DataStream<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
				.map(data -> {
					String[] split = data.split(",");
					return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
				});

		// 3.创建表执行环境
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 4.将流转换为动态表
		Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

		// 5.使用TableAPI进行查询
		// 5.1 建议新版表达式写法
		Table selectedTable = sensorTable.groupBy($("id"))
				.select($("id"),
						$("ts").count().as("ts_cnt"),
						$("vc").sum().as("vs_sum"));

		// 6. 声明输出表
		// 使用upsert sink时, document _id就是 groupby的主键,如果group by多个字段，此时_id默认就是使用_拼接,
		// 也可以通过keyDelimiter() 指定
		tableEnv.connect(new Elasticsearch()
				.index("sensor_sql")
				.documentType("_doc")
				.version("6")
				.host("node01", 9200, "http")
				.keyDelimiter("_")
				.bulkFlushMaxSize("42 mb")
				.bulkFlushInterval(120000L))
				.withSchema(
						new Schema()
								.field("id", DataTypes.STRING())
								.field("ts", DataTypes.BIGINT())
								.field("vc", DataTypes.INT()))
				.withFormat(new Json())
				.inUpsertMode()
				.createTemporaryTable("sensor");

		// 7. 输出到kafka topic
		selectedTable.executeInsert("sensorTable");

		// 8. 执行任务
		env.execute();
	}
}
