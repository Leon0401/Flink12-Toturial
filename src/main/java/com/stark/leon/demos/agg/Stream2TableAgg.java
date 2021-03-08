package com.stark.leon.demos.agg;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Vince
 */
public class Stream2TableAgg {
	public static void main(String[] args) throws Exception {
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
		Table selectedTable = sensorTable.where($("vc").isGreaterOrEqual(50))
				.groupBy($("id"))
				.aggregate($("vc").sum().as("sum_vc"))
				.select($("id"), $("sum_vc"));

		// 6.将结果转换为流进行输出
		// 由于同一个key的数据会变化，需要使用可撤回的流
		DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(selectedTable, Row.class);
		retractStream.print();

		env.execute();

		// nc64.exe -l -p 9999
//		"ws_100",1000,20
//		"ws_100",1000,40
//		"ws_100",1000,50
//		"ws_100",1000,30
	}
}
