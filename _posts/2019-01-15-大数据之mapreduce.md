---
layout: post
title: 大数据之mapreduce
subtitle: 
date: 2019-01-15
author: Salome
header-img: img/post-bg-2015.jpg
catalog: true
tags:

   - big data

---

# mapreduce概述

Maprecude是一个分布式运算程序的编程框架

- 优点

1. 良好的拓展性
2. 高容错

- 缺点

1. 不擅长实时计算
2. 不擅长流氏计算

- Mapreduce核心思想

![](https://tva1.sinaimg.cn/large/006tNbRwly1ga6obn71qnj31hw0u0ar7.jpg)

1）分布式的运算程序往往需要分成至少2个阶段。

2）第一个阶段的MapTask并发实例，完全并行运行，互不相干。

3）第二个阶段的ReduceTask并发实例互不相干，但是他们的数据依赖于上一个阶段的所有MapTask并发实例的输出。

4）MapReduce编程模型只能包含一个Map阶段和一个Reduce阶段，如果用户的业务逻辑非常复杂，那就只能多个MapReduce程序，串行运行。

一个完整的MapReduce程序在分布式运行时有三类实例进程

- MrAppMaster 负责整个程序的过程调度及状态协调
- MapTask 负责Map阶段的整个数据处理流程
- ReduceTask  负责Reduce阶段的整个数据处理流程

##### 编程规范

###### Map阶段2个步骤 

1. 设置 InputFormat 类, 将数据切分为 Key-Value(K1和V1) 对, 输入到第二步 
2. 自定义 Map 逻辑, 将第一步的结果转换成另外的 Key-Value(K2和V2) 对, 输出结果 

###### Shule 阶段 4 个步骤 

1. 对输出的 Key-Value 对进行 分区 
2. 对不同分区的数据按照相同的 Key 排序 
3. (可选) 对分组过的数据初步 规约 , 降低数据的网络拷贝 
4. 对数据进行 分组 , 相同 Key 的 Value 放入一个集合中 

###### Reduce 阶段 2 个步骤 

1. 对多个 Map 任务的结果进行排序以及合并, 编写 Reduce 函数实现自己的逻辑, 对输入的 Key-Value 进行处理, 转为新的 Key-Value(K3和V3)输出 

2. 设置 OutputFormat 处理并保存 Reduce 输出的 Key-Value 数据 

环境：Mac Pro、IDEA、jdk8

创建maven项目

在pom文件中添加如下依赖  

```xml
<dependencies>
    <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <version>RELEASE</version>
    </dependency>
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-core</artifactId>
        <version>2.8.2</version>
    </dependency>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-common</artifactId>
        <version>2.7.2</version>
    </dependency>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-client</artifactId>
        <version>2.7.2</version>
    </dependency>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-hdfs</artifactId>
        <version>2.7.2</version>
    </dependency>
</dependencies>
```

在项目的src/main/resources目录下，新建一个文件，命名为“log4j.properties”，在文件中填入。  

```properties
log4j.rootLogger=INFO, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d %p [%c] - %m%n
log4j.appender.logfile=org.apache.log4j.FileAppender
log4j.appender.logfile.File=target/spring.log
log4j.appender.logfile.layout=org.apache.log4j.PatternLayout
log4j.appender.logfile.layout.ConversionPattern=%d %p [%c] - %m%n
```

文件结构如下

<img src="https://tva1.sinaimg.cn/large/006tNbRwly1ga6spkv06cj30j20kwdhl.jpg" style="zoom: 33%;" />

Map

```java
package com.root.wordcount;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words) {

            k.set(word);
            context.write(k, v);
        }
    }
}
```

Reduce

```java
package com.root.wordcount;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    int sum;
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

        // 1 累加求和
        sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }

        // 2 输出
        v.set(sum);
        context.write(key,v);
    }
}
```

Driver

```java
package com.root.wordcount;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(WordCountDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
```

通过maven打包功能生成jar包，将jar包拷贝到Hadoop目录。启动集群，执行程序。

```
# 输入为/test.sql 输出为/output
[root@hadoop100 software]$ hadoop jar wc.jar
 com.root.wordcount.WordCountDriver /test.sql /oouput
```

结果查看命令hadoop fs -cat /oouput/*

# mapreduce原理

##### MapTask并行度决定机制

MapTask的并行度决定Map阶段的任务处理并发度，进而影响到整个Job的处理速度。

1G的数据，启动8个MapTask，可以提高集群的并发处理能力。那么1K的数据，也启动8个MapTask，会提高集群性能吗？MapTask并行任务是否越多越好呢？哪些因素影响了MapTask并行度？                                

**数据块：**Block是HDFS物理上把数据分成一块一块。

**数据切片：**数据切片只是在逻辑上对输入进行分片，并不会在磁盘上将其切分成片进行存储。

![](https://tva1.sinaimg.cn/large/006tNbRwly1ga6tg8hpduj31mi0u0k05.jpg)

##### FileInputFormat切片机制

针对每一个文件单独切片，切片大小默认等于Block大小。

##### CombineTextInputFormat

传统的FileInputFormat切片机制是对任务按文件规划切片，不管文件多小，都会是一个单独的切片，都会交给一个MapTask，这样如果有大量小文件，就会产生大量的MapTask，处理效率极其低下。

CombineTextInputFormat用于小文件过多的场景，它可以将多个小文件从逻辑上规划到一个切片中，这样，多个小文件就可以交给一个MapTask处理。

在驱动中添加代码如下：

```java
// 如果不设置InputFormat，它默认用的是TextInputFormat.class
job.setInputFormatClass(CombineTextInputFormat.class);
//虚拟存储切片最大值设置20m
CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);
```

##### shuffle机制

map方法之后，Reduce方法之前的数据处理过程是shuffle。

###### Partition分区

将统计结果按照条件输出到不同文件中，默认分区是根据key的hashcode对reduce tasks个数取模得到的。可以自定义Partition.  

1) 自定义类即成Partitioner,重写getPartition()方法

```java
class PartitonerOwn extends Partitioner<Text, LongWritable> {
	@Override
	public int getPartition(Text text, LongWritable longWritable, int i){
		if (text.toString().length()>5){
			return 0;
		}else {
			return 1;
		}
	}
}
```

2）在job驱动中设置自定义Partitioner

```java
job.setPartitionerClass(CustomPartitioner.class);
```

3) 根据自定义Partitioner的逻辑设置相应数量的ReduceTask

```java
job.setNumReuceTasks(2);
```

# 总体运行流程

![](https://tva1.sinaimg.cn/large/00831rSTgy1gdf6bh5a3tj32tg0j8444.jpg)

##### Map task

1. 读取数据组件 InputFormat (默认 TextInputFormat) 会通过 getSplits 方法对输入目录 中文件进行逻辑切片规划得到 block , 有多少个 block 就对应启动多少个 MapTask . 

2. 将输入文件切分为 block 之后, 由 RecordReader 对象 (默认是LineRecordReader) 进 行读取,以 \n 作为分隔符,读取一行数据,返回 <key，value>.Key表示每行首字符偏 移值, Value 表示这一行文本内容 

3. 读取 block 返回 <key,value> , 进入用户自己继承的 Mapper 类中，执行用户重写 的 map 函数, RecordReader 读取一行这里调用一次 

4. Mapper 逻辑结束之后, 将 Mapper 的每条结果通过 context.write 进行collect数据收 集. 在 collect 中, 会先对其进行分区处理，默认使用 HashPartitioner 
5. 接下来, 会将数据写入内存, 内存中这片区域叫做环形缓冲区, 缓冲区的作用是批量收集 Mapper 结果, 减少磁盘 IO 的影响. 我们的 Key/Value 对以及 Partition 的结果都会被写入 缓冲区. 当然, 写入之前，Key 与 Value 值都会被序列化成字节数组 
6. 当溢写线程启动后, 需要对这 80MB 空间内的 Key 做排序 (Sort). 排序是 MapReduce 模型 默认的行为, 这里的排序也是对序列化的字节做的排序 
7. 如果 Job 设置过 Combiner, 那么现在就是使用 Combiner 的时候了. 将有相同 Key 的 Key/Value 对的 Value 加起来, 减少溢写到磁盘的数据量. Combiner 会优化 MapReduce 的中间结果, 所以它在整个模型中会多次使用 
8. 合并溢写文件, 每次溢写会在磁盘上生成一个临时文件 (写之前判断是否有 Combiner), 如 果 Mapper 的输出结果真的很大, 有多次这样的溢写发生, 磁盘上相应的就会有多个临时文 件存在. 当整个数据处理结束之后开始对磁盘中的临时文件进行 Merge 合并, 因为最终的 文件只有一个, 写入磁盘, 并且为这个文件提供了一个索引文件, 以记录每个reduce对应数 据的偏移量 

##### reduceTask

1. Copy阶段 ，简单地拉取数据。Reduce进程启动一些数据copy线程(Fetcher)，通过HTTP 方式请求maptask获取属于自己的文件。 
2. Merge阶段 。这里的merge如map端的merge动作，只是数组中存放的是不同map端 copy来的数值 
3. 合并排序 。把分散的数据合并成一个大的数据后，还会再对合并后的数据排序。 
4. 对排序后的键值对调用reduce方法 ，键相等的键值对调用一次reduce方法，每次调用会 产生零个或者多个键值对，最后把这些输出的键值对写入到HDFS文件中。 

##### shuffle

1. Collect阶段 :将MapTask的结果输出到默认大小为100M的环形缓冲区，保存的是 key/value，Partition 分区信息等。 

2. Spill阶段 :当内存中的数据量达到一定的阀值的时候，就会将数据写入本地磁盘， 在将数据写入磁盘之前需要对数据进行一次排序的操作，如果配置了 combiner，还会将 有相同分区号和 key 的数据进行排序。 

3. Merge阶段 :把所有溢出的临时文件进行一次合并操作，以确保一个MapTask最终只 产生一个中间数据文件。 

4. Copy阶段 :ReduceTask启动Fetcher线程到已经完成MapTask的节点上复制一份属于 自己的数据，这些数据默认会保存在内存的缓冲区中，当内存的缓冲区达到一定的阀值 的时候，就会将数据写到磁盘之上。 

5. Merge阶段 :在ReduceTask远程复制数据的同时，会在后台开启两个线程对内存到本 地的数据文件进行合并操作。 

6. Sort阶段 :在对数据进行合并的同时，会进行排序操作，由于MapTask阶段已经对数 据进行了局部的排序，ReduceTask 只需保证 Copy 的数据的最终整体有效性即可。

# Yarn资源调度器

Yarn是一个资源调度平台，负责为运算程序提供服务器运算资源，相当于一个分布式的操作系统平台，而MapReduce相当于是运行于操作系统上的应用程序。yarn核心出发点是为了分离资源管理与作业监控，实现分离的做法是拥有一个全局的资源管 理(ResourceManager，RM)，以及每个应用程序对应一个的应用管理器 (ApplicationMaster，AM) 

总结一句话就是说:yarn主要就是为了调度资源，管理任务等 其调度分为两个层级来说: 

一级调度管理: 计算资源管理(CPU,内存，网络IO，磁盘) 

二级调度管理:
 任务内部的计算模型管理 (AppMaster的任务精细化管理) 

##### 架构

![](https://tva1.sinaimg.cn/large/006tNbRwly1ga7mu3lxypj31da0u00zv.jpg)

1) ResourceManager

负责处理客户端请求,对各NM上的资源进行统一管理和调度。给ApplicationMaster分配空 闲的Container 运行并监控其运行状态。主要由两个组件构成:调度器和应用程序管理器。 调度器(Scheduler):调度器根据容量、队列等限制条件，将系统中的资源分配给各个正 在运行的应用程序。调度器仅根据各个应用程序的资源需求进行资源分配，而资源分配 单位是Container。Shceduler不负责监控或者跟踪应用程序的状态。总之，调度器根据应 用程序的资源要求，以及集群机器的资源情况，为应用程序分配封装在Container中的资 源。 应用程序管理器(Applications Manager):应用程序管理器负责管理整个系统中所有应用 程序，包括应用程序提交、与调度器协商资源以启动ApplicationMaster 、监控 ApplicationMaster运行状态并在失败时重新启动等，跟踪分给的Container的进度、状态也 是其职责。 

2）NodeManager

NodeManager 是每个节点上的资源和任务管理器。它会定时地向ResourceManager汇报本 节点上的资源使用情况和各个Container的运行状态;同时会接收并处理来自 ApplicationMaster 的Container 启动/停止等请求。

3）ApplicationMaster

用户提交的应用程序均包含一个ApplicationMaster ，负责应用的监控，跟踪应用执行状 态，重启失败任务等。ApplicationMaster是应用框架，它负责向ResourceManager协调资 源，并且与NodeManager协同工作完成Task的执行和监控。

4）Container

Container是YARN中的资源抽象，它封装了某个节点上的多维度资源，如内存、CPU、磁 盘、网络等，当ApplicationMaster向ResourceManager申请资源时，ResourceManager为 ApplicationMaster 返回的资源便是用Container 表示的。 

##### 工作机制

![](https://tva1.sinaimg.cn/large/006tNbRwly1ga7n1ymwq4j31iq0u0gyx.jpg)

（1）MR程序提交到客户端所在的节点。

（2）YarnRunner向ResourceManager申请一个Application。

（3）RM将该应用程序的资源路径返回给YarnRunner。

（4）该程序将运行所需资源提交到HDFS上。

（5）程序资源提交完毕后，申请运行mrAppMaster。

（6）RM将用户的请求初始化成一个Task。

（7）其中一个NodeManager领取到Task任务。

（8）该NodeManager创建容器Container，并产生MRAppmaster。

（9）Container从HDFS上拷贝资源到本地。

（10）MRAppmaster向RM 申请运行MapTask资源。

（11）RM将运行MapTask任务分配给另外两个NodeManager，另两个NodeManager分别领取任务并创建容器。

（12）MR向两个接收到任务的NodeManager发送程序启动脚本，这两个NodeManager分别启动MapTask，MapTask对数据分区排序。

（13）MrAppMaster等待所有MapTask运行完毕后，向RM申请容器，运行ReduceTask。

 （14）ReduceTask向MapTask获取相应分区的数据。

 （15）程序运行完毕后，MR会向RM申请注销自己。

##### 资源调度器

目前，Hadoop作业调度器主要有三种：FIFO、Capacity Scheduler和Fair Scheduler。Hadoop2.7.2默认的资源调度器是Capacity Scheduler。



