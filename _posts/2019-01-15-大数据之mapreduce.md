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

1. MapTask的并行度决定Map阶段的任务处理并发度，进而影响到整个Job的处理速度。

1G的数据，启动8个MapTask，可以提高集群的并发处理能力。那么1K的数据，也启动8个MapTask，会提高集群性能吗？MapTask并行任务是否越多越好呢？哪些因素影响了MapTask并行度？                                

2. MapTask并行度决定机制

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

- Partition分区

将统计结果按照条件输出到不同文件中，默认分区是根据key的hashcode对reduce tasks个数取模得到的。可以自定义Partition.  

1) 自定义类即成Partitioner,重写getPartition()方法

2）在job驱动中设置自定义Partitioner

```java
job.setPartitionerClass(CustomPartitioner.class);
```

3) 根据自定义Partitioner的逻辑设置相应数量的ReduceTask

```java
job.setNumReuceTasks(5);
```

# Yarn资源调度器

Yarn是一个资源调度平台，负责为运算程序提供服务器运算资源，相当于一个分布式的操作系统平台，而MapReduce相当于是运行于操作系统上的应用程序。

##### 架构

![](https://tva1.sinaimg.cn/large/006tNbRwly1ga7mu3lxypj31da0u00zv.jpg)

1) ResourceManager

处理客户端请求、监控NodeManager、启动监控ApplicationMaster、资源的分配与调度

2）NodeManager

管理单个节点的资源、处理来自ResourceManager的命令、处理来自ApplicationMaster的命令

3）ApplicationMaster

负责数据切分、为应用程序申请资源并分配给内部的任务

4）Container

是Yarn的资源抽象，封装了内存等资源。

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



