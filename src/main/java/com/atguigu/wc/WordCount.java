package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 批处理
 * @author houjie
 * @version V1.0
 * @date 2020/12/26 20:49
 */
public class WordCount {

  public static void main(String[] args) throws Exception {
    // 创建执行的环境
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // 从文件中读取数据
    DataSet<String> inputDataSource = env.readTextFile(
        "F:\\workspace-idea\\atguigu\\FlinkTutorial\\src\\main\\resources\\hello.txt");

    // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
    DataSet<Tuple2<String, Integer>> resultSet = inputDataSource
        .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, out) -> {
          String[] words = line.split(" ");
          for (String word : words) {
            out.collect(new Tuple2<>(word, 1));
          }
        })
        .returns(Types.TUPLE(Types.STRING, Types.INT))
        .groupBy(0) // 按照第一个位置的word分组
        .sum(1); // 将第二个位置上的数据求和

    resultSet.print();
  }
}
