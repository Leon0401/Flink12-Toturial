package com.stark.leon.demos;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @title: FlinkSQLTest
 * @author: Leon Stark
 * @date: 2021/3/13 22:17
 * @desc:
 * @msg:
 */
public class FlinkSQLTest {
	public static void main(String[] args) throws Exception {
		// 1. 获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 读取端口数据创建刘并转换为Java Bean
		DataStream<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
				.map(data -> {
					String[] split = data.split(",");
					return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
				});

		// 3. 创建表执行环境
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 4. 将流转换为动态表
		Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

		// 5. 使用SQL查询未注册的表
		Table query = tableEnv.sqlQuery("select id,ts,vc from " + sensorTable + " where id='ws_001'");

		// 6. 转换为流
		tableEnv.toAppendStream(sensorTable, Row.class).print();

		// 7. 执行
		env.execute();
	}
}
