/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.process;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The "Expiring State" exercise from the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
public class ExpiringStateExercise extends ExerciseBase {
	static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
	static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
		final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;           // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; 	// 10 minutes worth of events are served every second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
				.filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
				.keyBy(ride -> ride.rideId);

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
				.keyBy(fare -> fare.rideId);

		SingleOutputStreamOperator processed = rides
				.connect(fares)
				.process(new EnrichmentFunction());

		printOrTest(processed.getSideOutput(unmatchedFares));

		env.execute("ExpiringStateExercise (java)");
	}

	public static class EnrichmentFunction
			extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {


		private transient ValueState<TaxiRide> rideState;
		private transient ValueState<TaxiFare> fareState;
		private transient ValueState<Long> timerState;

		private final long timeout = Time.minutes(30).toMilliseconds();

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<TaxiRide> rideDesc =
					new ValueStateDescriptor<>("saved-ride", TaxiRide.class);
			ValueStateDescriptor<TaxiFare> fareDesc =
					new ValueStateDescriptor<>("saved-fare", TaxiFare.class);
			ValueStateDescriptor<Long> timerDesc =
					new ValueStateDescriptor<>("timer-ts", Long.class);

			rideState = getRuntimeContext().getState(rideDesc);
			fareState = getRuntimeContext().getState(fareDesc);
			timerState = getRuntimeContext().getState(timerDesc);
		}

		@Override
		public void processElement1(
				TaxiRide ride,
				Context ctx,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiFare fare = fareState.value();
			if (fare != null) {
				fareState.clear();
				Long t = timerState.value();
				if (t != null) {
					ctx.timerService().deleteEventTimeTimer(t);
					timerState.clear();
				}
				out.collect(Tuple2.of(ride, fare));
			} else {
				rideState.update(ride);

				long timerTs = ride.getEventTime() + timeout;
				ctx.timerService().registerEventTimeTimer(timerTs);
				timerState.update(timerTs);
			}
		}

		@Override
		public void processElement2(
				TaxiFare fare,
				Context ctx,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiRide ride = rideState.value();
			if (ride != null) {
				rideState.clear();
				Long t = timerState.value();
				if (t != null) {
					ctx.timerService().deleteEventTimeTimer(t);
					timerState.clear();
				}
				out.collect(Tuple2.of(ride, fare));
			} else {
				fareState.update(fare);

				long timerTs = fare.getEventTime() + timeout;
				ctx.timerService().registerEventTimeTimer(timerTs);
				timerState.update(timerTs);
			}
		}

		@Override
		public void onTimer(
				long timestamp,
				OnTimerContext ctx,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiRide ride = rideState.value();
			TaxiFare fare = fareState.value();

			if (ride != null && fare == null) {
				ctx.output(unmatchedRides, ride);
				rideState.clear();
			} else if (fare != null && ride == null) {
				ctx.output(unmatchedFares, fare);
				fareState.clear();
			}

			timerState.clear();
		}
	}
}