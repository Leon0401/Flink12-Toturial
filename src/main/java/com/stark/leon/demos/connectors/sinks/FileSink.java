package com.stark.leon.demos.connectors.sinks;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @title: FileSink
 * @author: Leon Stark
 * @date: 2021/3/10 10:36
 * @desc:
 * @msg:
 */
public class FileSink {
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
		Table selectedTable = sensorTable.where($("id").isEqual("ws_001"))
				.select($("id"), $("ts"), $("vc"));

		// 6. 声明输出表
		tableEnv.connect(new FileSystem().path("output/result.txt"))
				.withFormat(new Csv())
				.withSchema(new Schema()
						.field("id", DataTypes.STRING())
						.field("ts", DataTypes.BIGINT())
						.field("vc", DataTypes.INT())
				)
				.inAppendMode()
				.createTemporaryTable("sensorTable");

		// 7. 输出到文件系统
		selectedTable.executeInsert("sensorTable");

		// 8. 执行任务
		env.execute();
	}
}
