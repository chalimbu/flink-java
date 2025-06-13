package org.part2datastreams;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.legacy.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class EssentialStreams {

    public static void applicationTemplate() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> simpleNumberStream = env.fromData(12,3,4,5);
        // because fromSequece was deprecated

        simpleNumberStream.print();
        // it print the thread number and the number itselt

        // a the end
        env.execute();
    }

    public void demoTransformation() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> numbers = env.fromData(1,2,3,4,5);

        System.out.println("current parallelism "+env.getParallelism());
        env.setParallelism(2);
        System.out.println("current parralelims" + env.getParallelism());

        SingleOutputStreamOperator<Integer> doubleNumber = numbers.map(x-> x*2);

        SingleOutputStreamOperator<Integer> expandedNumber = numbers.flatMap(new FlatMapFunction());

        SingleOutputStreamOperator<Integer> filteredNumber = numbers.filter( x -> x %2 ==0)
                .setParallelism(4);

        final StreamingFileSink<Integer> sink = StreamingFileSink
                .forRowFormat(new Path("/Users/sebastian.zapata/Insync/sebitaszapata12@gmail.com/Google Drive/pc-personal/copia/estudio/flink/rock-the-jvm-flink/flink-java/src/main/resources"), new SimpleStringEncoder<Integer>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();


        filteredNumber.addSink(sink);
        env.execute();
    }

    class FlatMapFunction implements org.apache.flink.api.common.functions.FlatMapFunction<Integer,Integer>{

        @Override
        public void flatMap(Integer integer, Collector<Integer> collector) throws Exception {
            for ( int i = 0;i <3; i++){
                collector.collect(integer+i);
            }
        }
    }



    public static void main(String[] args) throws Exception {
        EssentialStreams es= new EssentialStreams();
        es.demoTransformation();
    }
}

