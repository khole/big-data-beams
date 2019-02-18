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
package khole.beams;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import com.google.gson.Gson;

public class TaxiTripPipeline {
//  private static final String INPUT_FILE = "gs://drivemode-taxi-trip/large_df.json";
//  private static final String INPUT_FILE = "./raw-data/large_df.json";
  private static final String INPUT_FILE = "./raw-data/chunk_3.json";
  private static final String OUTPUT_FILE_PREFIX = "taxi_trip-3";
//  private static final String INPUT_FILE = "./raw-data/large_df.zip";
//  private static final String INPUT_FILE = "./raw-data/small_df.json";
//   private static final String INPUT_FILE = "./chunk_2M";
//   private static final String INPUT_FILE = "./chunk_1M";
//  private static final String INPUT_FILE = "./chunk_500k";
//  private static final String INPUT_FILE = "./chunk_100k";
//  private static final String INPUT_FILE = "./chunk_1k";
  private static final int SHARDS = 1;
//  private static final int LINES_PER_WINDOW = 100;
  private static final int LINES_PER_WINDOW = 20000;
  private static final int HOURS = 3;
  private static final String COMMA = ",";
  private final static String CSV_HEADER = "uuid,pickup_datetime,dropoff_datetime,store_and_fwd_flag,rate_code"
        + ",pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,passenger_count,fare_amount"
        + ",tip_amount,tolls_amount,total_amount,payment_type,distance_between_service,time_between_service"
        + ",trip_type";
  
//{"Uuid":"072e3d94-c4e6-4852-baef-db15c951216d","Pickup datetime":"2015-02-04 17:38:47 UTC",
//"Dropoff datetime":"2015-02-04 17:38:57 UTC",
//"Store and fwd flag":"N","Rate code":1,
//"Pickup longitude":-73.9452362061,"Pickup latitude":40.7026481628,
//"Dropoff longitude":-73.9447784424,"Dropoff latitude":40.7023582458,
//"Passenger count":1,"Fare amount":-2.5,"Tip amount":0.0,"Tolls amount":0.0,
//"Total amount":-4.3,"Payment type":4,"Distance between service":29,
//"Time between service":65644,"Trip type":1.0}
  
   static void runWindowedTaxiTripPipeline() throws IOException
   {
      PipelineOptions options = PipelineOptionsFactory.create();
      final Instant minTimestamp = new Instant(System.currentTimeMillis());
      final Instant maxTimestamp = new Instant(System.currentTimeMillis() + Duration.standardHours(HOURS).getMillis());
      Pipeline pipeline = Pipeline.create(options);
      
//      PCollection<String> input = pipeline.apply(TextIO.read().withCompression(Compression.ZIP).from(INPUT_FILE))
      PCollection<String> input = pipeline.apply(TextIO.read().from(INPUT_FILE))
            .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));
//      PCollection<String> windowedTaxiTrips = input.apply("Window", Window
//             .<String>into(new GlobalWindows())
//             .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(LINES_PER_WINDOW)))
//             .withAllowedLateness(Duration.ZERO)
//             .discardingFiredPanes()
//         );
            
          PCollection<String> windowedTaxiTrips =
          input.apply(
              Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
//                  .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(LINES_PER_WINDOW)))
//                  .withAllowedLateness(Duration.ZERO)
//                  .discardingFiredPanes()
           );
      
      windowedTaxiTrips.apply(ParDo.of(new JsonToObjectFn()))
                       .apply(ParDo.of(new FilterNullFn()))
                       .apply(MapElements.via(new FormatAsCSVFn()))
                       .apply(new WriteOneCsvPerWindow(OUTPUT_FILE_PREFIX, SHARDS));
      
      PipelineResult result = pipeline.run();
      try
      {
         result.waitUntilFinish();
      }
      catch (Exception exc)
      {
         result.cancel();
      }
   }
   
   static void runUnwindowedTaxiTripPipeline() {
      PipelineOptions options = PipelineOptionsFactory.create();

      Pipeline p = Pipeline.create(options);

      p.apply(TextIO.read().from(INPUT_FILE))
       .apply(ParDo.of(new JsonToObjectFn()))
       .apply(MapElements.via(new FormatAsCSVFn()))
       
          .apply(TextIO.write().to("taxiTrip")
                 .withHeader(CSV_HEADER)
                 .withSuffix(".csv")
//                 .withWritableByteChannelFactory(FileBasedSink.CompressionType.GZIP)
                 );

      p.run().waitUntilFinish();
   }

   public static void main(String[] args)
   {    
      try
      {
         // runUnwindowedTaxiTripPipeline();
         runWindowedTaxiTripPipeline();
      }
      catch (IOException e)
      {
         e.printStackTrace();
      }
   }
  public static class JsonToObjectFn extends DoFn<String, TaxiTrip> {
      @ProcessElement
      public void processElement(@Element String element, OutputReceiver<TaxiTrip> receiver)
      {
         TaxiTrip taxiTrip = new Gson().fromJson(element, TaxiTrip.class);
         receiver.output(taxiTrip);
      }
  }
  public static class FilterNullFn extends DoFn<TaxiTrip, TaxiTrip> {
     @ProcessElement
     public void processElement(@Element TaxiTrip taxiTrip, OutputReceiver<TaxiTrip> receiver)
     {
        if (taxiTrip.getUuid() !=null && !taxiTrip.getUuid().isEmpty()
              && taxiTrip.getPickupDatetime() !=null && !taxiTrip.getPickupDatetime().isEmpty()
              && taxiTrip.getDropoffDatetime() !=null && !taxiTrip.getDropoffDatetime().isEmpty()
              && taxiTrip.getStoreAndFwdFlag() !=null && !taxiTrip.getStoreAndFwdFlag().isEmpty()
              && taxiTrip.getRateCode() !=null && !taxiTrip.getRateCode().isEmpty()
              && taxiTrip.getPickupLongitude() != null && taxiTrip.getPickupLongitude() != 0
              && taxiTrip.getPickupLatitude() != null && taxiTrip.getPickupLatitude() != 0
              && taxiTrip.getDropoffLongitude() != null && taxiTrip.getDropoffLongitude() != 0
              && taxiTrip.getDropoffLatitude() != null && taxiTrip.getDropoffLatitude() != 0
              && taxiTrip.getPassengerCount() != null && taxiTrip.getPassengerCount() != 0
              && taxiTrip.getFareAmount() != null && taxiTrip.getFareAmount() > 0
              && taxiTrip.getTipAmount() != null && taxiTrip.getTipAmount() >= 0
              && taxiTrip.getTollsAmount() != null && taxiTrip.getTollsAmount() >= 0
              && taxiTrip.getTotalAmount() != null && taxiTrip.getTotalAmount() > 0
              && taxiTrip.getPaymentType() !=null && !taxiTrip.getPaymentType().isEmpty()
              && taxiTrip.getDistanceBetweenService() != null && taxiTrip.getTotalAmount() > 0
              && taxiTrip.getTimeBetweenService() != null && taxiTrip.getTimeBetweenService() > 0
              && taxiTrip.getTripType() != null && !taxiTrip.getTripType().isEmpty()   
        ) {
           receiver.output(taxiTrip);
        } 
     }
  }
  public static class FormatAsCSVFn extends SimpleFunction<TaxiTrip, String> {
     @Override
     public String apply(TaxiTrip taxiTrip) {
        StringBuilder csvRow = new StringBuilder();
        csvRow.append(taxiTrip.getUuid()).append(COMMA)
              .append(taxiTrip.getPickupDatetime()).append(COMMA)
              .append(taxiTrip.getDropoffDatetime()).append(COMMA)
              .append(taxiTrip.getStoreAndFwdFlag()).append(COMMA)
              .append(taxiTrip.getRateCode()).append(COMMA)
              .append(taxiTrip.getPickupLongitude()).append(COMMA)
              .append(taxiTrip.getPickupLatitude()).append(COMMA)
              .append(taxiTrip.getDropoffLongitude()).append(COMMA)
              .append(taxiTrip.getDropoffLatitude()).append(COMMA)
              .append(taxiTrip.getPassengerCount()).append(COMMA)
              .append(taxiTrip.getFareAmount()).append(COMMA)
              .append(taxiTrip.getTipAmount()).append(COMMA)
              .append(taxiTrip.getTollsAmount()).append(COMMA)
              .append(taxiTrip.getTotalAmount()).append(COMMA)
              .append(taxiTrip.getPaymentType()).append(COMMA)
              .append(taxiTrip.getDistanceBetweenService()).append(COMMA)
              .append(taxiTrip.getTimeBetweenService()).append(COMMA)
              .append(taxiTrip.getTripType());
       
       return csvRow.toString();
     }
   }
  
  static class AddTimestampFn extends DoFn<String, String> {
     private final Instant minTimestamp;
     private final Instant maxTimestamp;

     AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
       this.minTimestamp = minTimestamp;
       this.maxTimestamp = maxTimestamp;
     }

     @ProcessElement
     public void processElement(@Element String element, OutputReceiver<String> receiver) {
       Instant randomTimestamp =
           new Instant(
               ThreadLocalRandom.current()
                   .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));
       receiver.outputWithTimestamp(element, new Instant(randomTimestamp));
     }
   }

}
//split -l 1000000 raw-data/large_df.json chunk_
//wc -l raw-data/large_df.json ->3810150
//export MAVEN_OPTS="-Xmx12000m -XX:-UseGCOverheadLimit -XX:+UseStringDeduplication"
//mvn compile exec:java -Dexec.mainClass=khole.beams.TaxiTripPipeline 