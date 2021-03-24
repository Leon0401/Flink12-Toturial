package com.stark.leon.demos.time.attr;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @title: ProcessTimeAttribute
 * @author: Leon Stark
 * @date: 2021/3/24 20:46
 * @desc:
 *          DataStream 转 Table 时指定时间字段
 * @msg:
 */
public class ProcessTimeAttribute {
	public static void main(String[] args) {
		// 1. 获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		SingleOutputStreamOperator<WaterSensor> sensorDs = env.readTextFile("input/sensors.txt")
				.map(msg -> {
					String[] split = msg.split(",");
					return new WaterSensor(split[0],
							Long.parseLong(split[1]),
							Integer.parseInt(split[2]));
				});

		// 声明处理时间
		Table sensorTable = tableEnv.fromDataStream(sensorDs,
				$("id"),
				$("ts"),
				$("vc"),
				$("pt").proctime());
		sensorTable.printSchema();
	}
}
