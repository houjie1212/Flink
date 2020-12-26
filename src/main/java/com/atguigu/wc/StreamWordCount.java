package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流处理
 * @author houjie
 * @version V1.0
 * @date 2020/12/26 21:26
 */
public class StreamWordCount {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2); // 设置并发线程数

//    DataStream<String> inputDataStreamSource = env.readTextFile(
//        "F:\\workspace-idea\\atguigu\\FlinkTutorial\\src\\main\\resources\\hello.txt");

    // bash: nc -lk 7777
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String host = parameterTool.get("host");
    int port = parameterTool.getInt("port");
    DataStream<String> inputDataStreamSource = env.socketTextStream(host, port);

    DataStream<Tuple2<String, Integer>> resultStream = inputDataStreamSource
        .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, out) -> {
          String[] words = line.split(" ");
          for (String word : words) {
            out.collect(new Tuple2<>(word, 1));
          }
        })
        .returns(Types.TUPLE(Types.STRING, Types.INT))
        .keyBy(0)
        .sum(1);

    resultStream.print();

    // 执行任务
    env.execute();
  }
}
