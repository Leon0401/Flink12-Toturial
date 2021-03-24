package com.stark.leon.demos.time.attr;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @title: ProcessTimeAttribute
 * @author: Leon Stark
 * @date: 2021/3/24 20:46
 * @desc: 创建表的DDL中指定时间字段
 * @msg:
 */
public class ProcessTimeAttributeOnDDL {
	public static void main(String[] args) {
		// 1. 获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 2. ddl中指定时间字段
		tableEnv.executeSql("create table sensor (" +
				"id string," +
				"ts bigint," +
				"vc int," +
				"pt_time as PROCTIME()" +
				") with("
				+ "'connector' = 'filesystem',"
				+ "'path' = 'input/sensors.txt',"
				+ "'format' = 'csv'"
				+ ")"
		);


		// 3. 生成动态表
		Table sensorTable = tableEnv.from("sensor");

		sensorTable.printSchema();
	}
}
