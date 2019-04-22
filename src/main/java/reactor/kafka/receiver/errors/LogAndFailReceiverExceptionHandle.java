/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.kafka.receiver.errors;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxSink;

/**
 * Receiver exception handler that logs an exception and then fails.
 */
public class  LogAndFailReceiverExceptionHandle<K, V> implements ReceiverExceptionHandler<K, V> {

    private static final Logger log = LoggerFactory.getLogger(LogAndFailReceiverExceptionHandle.class);

    @Override
    public void handle(FluxSink<ConsumerRecords<K, V>> recordSubmission, Exception exception) {
        log.error("Receiver exception caught: {}",
                exception.getLocalizedMessage(),
                exception);

        recordSubmission.error(exception);
    }
}
