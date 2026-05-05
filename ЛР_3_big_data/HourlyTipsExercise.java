/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /** Creates a job using the source and sink provided. */
    public HourlyTipsExercise(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsExercise job =
                new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();

    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<TaxiFare> fares = env
                .addSource(source)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TaxiFare>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(60))
                                .withTimestampAssigner((fare, ts) -> fare.getEventTimeMillis())
                );


        DataStream<Tuple3<Long, Long, Float>> hourlyTips =
                fares
                        .keyBy(f -> f.driverId)
                        .window(TumblingEventTimeWindows.of(Time.hours(1)))
                        .process(new SumTipsPerDriverPerHour());


        DataStream<Tuple3<Long, Long, Float>> hourlyMax =
                hourlyTips
                        .keyBy(t -> t.f0)
                        .window(TumblingEventTimeWindows.of(Time.hours(1)))
                        .maxBy(2);
        hourlyMax.addSink(sink);

        return env.execute("Hourly Tips");
    }

    public static class SumTipsPerDriverPerHour
            extends ProcessWindowFunction<
            TaxiFare,                    // вход
            Tuple3<Long, Long, Float>,   // выход
            Long,                        // ключ = driverId
            TimeWindow> {

        @Override
        public void process(
                Long driverId,
                Context ctx,
                Iterable<TaxiFare> fares,
                Collector<Tuple3<Long, Long, Float>> out) {

            float sum = 0F;
            for (TaxiFare f : fares) {
                sum += f.tip;
            }

            long windowEnd = ctx.window().getEnd();
            out.collect(Tuple3.of(windowEnd, driverId, sum));
        }
    }
}
