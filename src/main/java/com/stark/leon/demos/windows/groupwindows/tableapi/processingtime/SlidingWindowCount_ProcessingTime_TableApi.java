package com.stark.leon.demos.windows.groupwindows.tableapi.processingtime;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @title: TumbleWindowInstance_ProcessingTime_TableApi
 * @author: Leon Stark
 * @date: 2021/5/9 9:44
 * @desc: Group Windows : 分组窗口-- 基于时间 （滚动、滑动、会话）
 *      分组窗口（Group Windows）会根据时间或行计数间隔，将行聚合到有限的组（Group）中，并对每个组的数据执行一次聚合函数。
 *      Table API中的Group Windows都是使用。Window（w:GroupWindow）子句定义的，并且必须由as子句指定一个别名。
 *      为了按窗口对表进行分组，窗口的别名必须在group by子句中，像常规的分组字段一样引用。
 * @msg:  nc64.exe -l -p 9999
 * @ref: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/tableApi.html#group-windows
 */
public class SlidingWindowCount_ProcessingTime_TableApi {
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

		// 3. 将流转换为表并指定 处理时间
		Table table = tableEnv.fromDataStream(waterSensorDS,
				$("id"),
				$("ts"),
				$("vc"),
				$("pt").proctime()
		);

		// 4. 基于 处理时间 的 滑动计数窗口
		/**
		 *  // Sliding Event-time Window
		 * .window(Slide.over(lit(10).minutes())
		 *             .every(lit(5).minutes())
		 *             .on($("rowtime"))
		 *             .as("w"));
		 *
		 * // Sliding Processing-time window (assuming a processing-time attribute "proctime")
		 * .window(Slide.over(lit(10).minutes())
		 *             .every(lit(5).minutes())
		 *             .on($("proctime"))
		 *             .as("w"));
		 *
		 * // Sliding Row-count window (assuming a processing-time attribute "proctime")
		 * .window(Slide.over(rowInterval(10)).every(rowInterval(5)).on($("proctime")).as("w"));
		 */
		Table result = table.window(Slide.over(rowInterval(5L)).every(rowInterval(2L)).on($("pt")).as("tw"))
				.groupBy($("id"), $("tw"))
				.select($("id"), $("id").count());

		// 5. 将结果表转换为流输出 (由于滚动窗口不涉及到修改，每个窗口是独立的，也就是只需要使用追加流即可)
		tableEnv.toAppendStream(result, Row.class).print();

		// 6. 执行
		env.execute();
	}
}
