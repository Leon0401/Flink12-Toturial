a simple cookbook for learning flink 
#### File Sink
[File Sink](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/connectors/file_sink.html#file-formats) 

######  01 概述     
        1.这个连接器提供了一个在流和批模式下统一的 Sink 来将分区文件写入到支持 Flink FileSystem 接口的文件系统中，它对于流和批模式可以提供相同的一致性语义保证。
    File Sink 是现有的 Streaming File Sink 的一个升级版本，后者仅在流模式下提供了精确一致性。
        2.File Sink 会将数据写入到桶中。由于输入流可能是无界的，因此每个桶中的数据被划分为多个有限大小的文件。
    如何分桶是可以配置的，默认使用基于时间的分桶策略，这种策略每个小时创建一个新的桶，桶中包含的文件将记录所有该小时内从流中接收到的数据。
        3.桶目录中的实际输出数据会被划分为多个part file，每一个接收桶数据的 Sink Subtask ，至少包含一个part file。
    额外的part file将根据滚动策略创建，滚动策略是可以配置的。对于 行编码格式(Row-encoded Formats) 默认的策略是根据文件大小和超时时间来滚动文件。
    超时时间指打开文件的最长持续时间，以及文件关闭前的最长非活动时间。批量编码格式(Bulk-encoded Formats) 必须在每次 Checkpoint 时滚动文件，但是用户也可以指定额外的基于文件大小和超时时间的策略。

    ·NOTE： 在流模式下使用 FileSink 时需要启用 Checkpoint ，每次做 Checkpoint 时写入完成。
            如果 Checkpoint 被禁用，部分文件（part file）将永远处于 'in-progress' 或 'pending' 状态，下游系统无法安全地读取。 
            
##### 02 文件格式
   FileSink 支持行编码格式和批量编码格式，比如 Apache Parquet 。 这两种变体随附了各自的构建器，可以使用以下静态方法创建：
```
    Row-encoded sink: FileSink.forRowFormat(basePath, rowEncoder)
    Bulk-encoded sink: FileSink.forBulkFormat(basePath, bulkWriterFactory)
```
   创建行或批量编码的 Sink 时，我们需要指定存储桶的基本路径和数据的编码逻辑。
    

创建行或批量编码的 Sink 时，我们需要指定存储桶的基本路径和数据的编码逻辑。

###### 2.1 Row-encoded Formats
   行编码格式需要指定一个 Encoder 。Encoder 负责为每个处于 In-progress 状态文件的OutputStream 序列化数据。
   
   除了桶分配器之外，RowFormatBuilder 还允许用户指定：
   
       Custom RollingPolicy ：自定义滚动策略以覆盖默认的 DefaultRollingPolicy。
       bucketCheckInterval （默认为1分钟）：毫秒间隔，用于基于时间的滚动策略。
   
   字符串元素写入示例：
```
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

DataStream<String> input = ...;

final FileSink<String> sink = FileSink
    .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
    .withRollingPolicy(
        DefaultRollingPolicy.builder()
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
            .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
            .withMaxPartSize(1024 * 1024 * 1024)
            .build())
	.build();

input.sinkTo(sink);
``` 
这个例子创建了一个简单的 Sink ，将记录分配给默认的一小时时间桶。它还指定了一个滚动策略，该策略在以下三种情况下滚动处于 In-progress 状态的部分文件（part file）：

    它至少包含 15 分钟的数据
    最近 5 分钟没有收到新的记录
    文件大小达到 1GB （写入最后一条记录后）
    

#### window
##### Group Window & Over Window
```markdown
 滚动、滑动的Group Window 只支持处理时间的计数窗口
```