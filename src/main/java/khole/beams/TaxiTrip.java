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

import org.apache.beam.examples.DebuggingWordCount;
import org.apache.beam.examples.WindowedWordCount;
import org.apache.beam.examples.WordCount;
import org.apache.beam.examples.WordCount.FormatAsTextFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link TaxiTrip}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class TaxiTrip {

  public static void main(String[] args) {

    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.
    PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific changes:
    //
    // CHANGE 1/3: Select a Beam runner, such as BlockingDataflowRunner
    // or FlinkRunner.
    // CHANGE 2/3: Specify runner-required options.
    // For BlockingDataflowRunner, set project and temp location as follows:
    //   DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    //   dataflowOptions.setRunner(BlockingDataflowRunner.class);
    //   dataflowOptions.setProject("SET_YOUR_PROJECT_ID_HERE");
    //   dataflowOptions.setTempLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY");
    // For FlinkRunner, set the runner as follows. See {@code FlinkPipelineOptions}
    // for more details.
    //   options.as(FlinkPipelineOptions.class)
    //      .setRunner(FlinkRunner.class);

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is one line from
    // the input text (a set of Shakespeare's texts).
    
    Schema taxiTrip =
          Schema.builder()
              .addStringField("Uuid")
              .addStringField("Pickup datetime")
              .addStringField("Dropoff datetime")
              .addStringField("Store and fwd flag")
              .addInt32Field("Rate code")
              .addDoubleField("Pickup longitude")
              .addDoubleField("Pickup latitude")
              .addDoubleField("Dropoff longitude")
              .addDoubleField("Dropoff latitude")
              .addInt32Field("Passenger count")
              .addDoubleField("Fare amount")
              .addDoubleField("Tip amount")
              .addDoubleField("Tolls amount")
              .addDoubleField("Total amount")
              .addInt32Field("Payment type")
              .addDoubleField("Distance between service")
              .addInt32Field("Time between service")
              .addDoubleField("Trip type")
              .build();

    // This example reads a public data set consisting of the complete works of Shakespeare.
//    p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))
//    p.apply(TextIO.read().from("gs://drivemode-taxi-trip/*"))
    p.apply(TextIO.read().from("./raw-data/large_df.json"))
//    p.apply(TextIO.read().from("./raw-data/small_df.json"))
     .apply(JsonToRow.withSchema(taxiTrip))
     .apply(MapElements.via(new FormatAsCSVFn()))

    
//    {"Uuid":"072e3d94-c4e6-4852-baef-db15c951216d","Pickup datetime":"2015-02-04 17:38:47 UTC",
//       "Dropoff datetime":"2015-02-04 17:38:57 UTC",
//       "Store and fwd flag":"N","Rate code":1,
//       "Pickup longitude":-73.9452362061,"Pickup latitude":40.7026481628,
//       "Dropoff longitude":-73.9447784424,"Dropoff latitude":40.7023582458,
//       "Passenger count":1,"Fare amount":-2.5,"Tip amount":0.0,"Tolls amount":0.0,
//       "Total amount":-4.3,"Payment type":4,"Distance between service":29,
//       "Time between service":65644,"Trip type":1.0}
    
        // Concept #2: Apply a FlatMapElements transform the PCollection of text lines.
        // This transform splits the lines in PCollection<String>, where each element is an
        // individual word in Shakespeare's collected texts.
//        .apply(
//            FlatMapElements.into(TypeDescriptors.strings())
//                .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
        // We use a Filter transform to avoid empty word
//        .apply(Filter.by((String word) -> !word.isEmpty()))
        // Concept #3: Apply the Count transform to our PCollection of individual words. The Count
        // transform returns a new PCollection of key/value pairs, where each key represents a
        // unique word in the text. The associated value is the occurrence count for that word.
//        .apply(Count.perElement())
        // Apply a MapElements transform that formats our PCollection of word counts into a
        // printable string, suitable for writing to an output file.
//        .apply(
//            MapElements.into(TypeDescriptors.strings())
//                .via(
//                    (KV<String, Long> wordCount) ->
//                        wordCount.getKey() + ": " + wordCount.getValue()))
        // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
        // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
        // formatted strings) to a series of text files.
//   .apply(
//   MapElements.into(TypeDescriptors.strings())
//       .via(
//           (KV<String, Long> wordCount) ->
//               wordCount.getKey() + ": " + wordCount.getValue()))
        // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        .apply(TextIO.write().to("taxiTrip"));

    p.run().waitUntilFinish();
  }
  
  public static class FormatAsCSVFn extends SimpleFunction<Row, String> {
     @Override
     public String apply(Row row) {
        StringBuilder csvRow = new StringBuilder();
        csvRow.append(row.getString("Uuid")).append(",")
              .append(row.getString("Pickup datetime")).append(",")
              .append(row.getString("Dropoff datetime")).append(",")
              .append(row.getString("Store and fwd flag")).append(",")
              .append(row.getInt32("Rate code")).append(",")
              .append(row.getDouble("Pickup longitude")).append(",")
              .append(row.getDouble("Pickup latitude")).append(",")
              .append(row.getDouble("Dropoff longitude")).append(",")
              .append(row.getDouble("Dropoff latitude")).append(",")
              .append(row.getInt32("Passenger count")).append(",")
              .append(row.getDouble("Fare amount")).append(",")
              .append(row.getDouble("Tip amount")).append(",")
              .append(row.getDouble("Tolls amount")).append(",")
              .append(row.getDouble("Total amount")).append(",")
              .append(row.getInt32("Payment type")).append(",")
              .append(row.getDouble("Distance between service")).append(",")
              .append(row.getInt32("Time between service")).append(",")
              .append(row.getDouble("Trip type"));
       
       return csvRow.toString();
     }
   }
  
//  mvn compile exec:java -Dexec.mainClass=khole.beams.TaxiTrip
}
