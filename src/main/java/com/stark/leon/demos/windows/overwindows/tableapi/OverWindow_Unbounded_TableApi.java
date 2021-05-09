package com.stark.leon.demos.windows.overwindows.tableapi;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_RANGE;

/**
 * @title: OverWindow_Unbounded_TableApi
 * @author: Leon Stark
 * @date: 2021/5/9 11:22
 * @desc:
 * @msg:
 * @ref: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/tableApi.html#over-windows
 */
public class OverWindow_Unbounded_TableApi {
	public static void main(String[] args) throws Exception {
		// 1. 获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 2. 读取端口数据创建刘并转换为Java Bean
		DataStream<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
				.map(data -> {
					String[] split = data.split(",");
					return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
				});

		// 3. 将流转换为表并指定处理时间
		Table table = tableEnv.fromDataStream(waterSensorDS,
				$("id"),
				$("ts"),
				$("vc"),
				$("pt").proctime()
		);

		// 4. 开启 无界窗口
		/**
		 * // Unbounded Event-time over window (assuming an event-time attribute "rowtime")
		 * .window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_RANGE).as("w"));
		 *
		 * // Unbounded Processing-time over window (assuming a processing-time attribute "proctime")
		 * .window(Over.partitionBy($("a")).orderBy("proctime").preceding(UNBOUNDED_RANGE).as("w"));
		 *
		 * // Unbounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
		 * .window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_ROW).as("w"));
		 *
		 * // Unbounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
		 * .window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(UNBOUNDED_ROW).as("w"));
		 */
		// 4.1 窗口大小为 ： 从第一行到当前行
		Table result1 = table.window(Over.partitionBy($("id")).orderBy($("pt")).preceding(UNBOUNDED_RANGE).as("ow"))
				.select($("id"),
						$("vc").sum().over($("ow")),
						$("ts").max().over($("ow"))
				);

		// 默认就是无界窗口
		Table result = table.window(Over.partitionBy($("id")).orderBy($("pt")).as("ow"))
				.select($("id"),
						$("vc").sum().over($("ow")),
						$("ts").max().over($("ow"))
				);

		// 4.2 指定行到当前行
		Table result2 = table.window(Over.partitionBy($("id")).orderBy($("pt")).preceding(UNBOUNDED_RANGE).as("ow"))
				.select($("id"),
						$("vc").sum().over($("ow")),
						$("ts").max().over($("ow"))
				);

		// 5. 将结果表转换为流进行输出
		tableEnv.toAppendStream(result, Row.class).print();

		// 6. 执行
		env.execute();

	}
}
