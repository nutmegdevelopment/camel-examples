/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.example;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.Expression;
import org.apache.camel.Predicate;
import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.apache.camel.component.aws2.s3.AWS2S3Constants;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.apache.camel.support.processor.idempotent.MemoryIdempotentRepository;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * To use the endpoint DSL then we must extend EndpointRouteBuilder instead of RouteBuilder
 */
public class MyRouteBuilder extends EndpointRouteBuilder {

    //simply combines Exchange String body values using '+' as a delimiter
    class StringAggregationStrategy implements AggregationStrategy {

        public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
            if (oldExchange == null) {
                return newExchange;
            }

            String oldBody = oldExchange.getIn().getBody(String.class);
            String newBody = newExchange.getIn().getBody(String.class);
            oldExchange.getIn().setBody(oldBody + System.lineSeparator() + newBody);
            return oldExchange;
        }
    }

    @Override
    public void configure() throws Exception {

//        // TODO - CSV format
//        CsvDataFormat csvDataFormat = new CsvDataFormat();
//        csvDataFormat.setHeader(new String[] { "bla", "blabla" });
//        csvDataFormat.setDelimiter(',');


        // Controls what criteria to use to aggregate messages
        // We can use body().getXYZ() methods to get the key from
        // the record if we wanted to batch by key
        Expression allExpr = constant("ALL");

        long maxWait = TimeUnit.SECONDS.toMillis(30);
        int maxSize = 10;

        from(kafka("{{kafkaTopic}}")
                    .brokers("{{kafkaBrokers}}")
                    .groupId(UUID.randomUUID().toString()) // starting from scratch in every execution
                    .autoOffsetReset("earliest")
                    .allowManualCommit(true)
                    .consumerStreams(1)
                    .schemaRegistryURL("{{schemaRegistryUrl}}")
                    .keyDeserializer(StringDeserializer.class.getName())
                    .valueDeserializer(io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName()))

                .aggregate(allExpr, new StringAggregationStrategy()).completionInterval(maxWait).completionSize(maxSize)
                .process(exchange -> {
                    // Assign file name
                    String fileName = String.format("myprefix-%s.csv",
                            LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
                    exchange.getIn().setHeader(AWS2S3Constants.KEY, fileName);

                    log("Filename generated: " + fileName);
                })
                .log("Uploading to S3 file {{sourceBucketName}}/${header.CamelAwsS3Key} to S3")
                .to(aws2S3("{{sourceBucketName}}"))
                .process(exchange -> {
                    log("About to commit offsets");
                    KafkaManualCommit manual =
                            exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
                    manual.commitSync();
                    log("Offsets committed");
                });

//        from(aws2S3("{{sourceBucketName}}").delay(1000L).deleteAfterRead(false).moveAfterRead(true).destinationBucket("{{processedBucketName}}"))
//                .idempotentConsumer(header("CamelAwsS3ETag"), MemoryIdempotentRepository.memoryIdempotentRepository())
//            .log("Uploading to SFTP server file {{sourceBucketName}}/${header.CamelAwsS3Key} with content:\n${body}\n\n")
//            .to(sftp("{{sftpPath}}").privateKeyFile("{{sftpPrivateKeyPath}}").username("{{sftpUsername").advanced().autoCreate(true));
    }
}
