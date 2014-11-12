/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich
package kinesis
package sources

import org.apache.commons.codec.binary.Base64

// Amazon
import com.amazonaws.auth._

// Scalaz
import scalaz.{Sink => _, _}
import Scalaz._

// Snowplow
import sinks._
import com.snowplowanalytics.maxmind.geoip.IpGeo
import common.outputs.{EnrichedEvent, BadRow}
import common.loaders.ThriftLoader
import com.snowplowanalytics.snowplow.enrich.common.{EtlPipeline, ValidatedMaybeCollectorPayload}
import common.enrichments.EnrichmentManager
import common.enrichments.registry.AnonOctets

// Iglu
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._

import common.loaders.ThriftLoader
import common.enrichments.EnrichmentRegistry
import common.enrichments.EnrichmentManager
import common.adapters.AdapterRegistry

/**
 * Abstract base for the different sources
 * we support.
 */
abstract class AbstractSource(config: KinesisEnrichConfig, igluResolver: Resolver, enrichmentRegistry: EnrichmentRegistry) {
  
  /**
   * Never-ending processing loop over source stream.
   */
  def run

  implicit val resolver: Resolver = igluResolver

  /**
   * Fields in our CanonicalOutput which are discarded for legacy
   * Redshift space reasons
   */
  private val DiscardedFields = Array("page_url", "page_referrer")

  // Initialize a kinesis provider to use with a Kinesis source or sink.
  protected val kinesisProvider = createKinesisProvider

  // Initialize the sink to output enriched events to.
  protected val sink: Option[ISink] = config.sink match {
    case Sink.Kinesis => new KinesisSink(kinesisProvider, config, InputType.Good).some
    case Sink.Stdouterr => new StdouterrSink(InputType.Good).some
    case Sink.Test => None
  }

  protected val badSink: Option[ISink] = config.sink match {
    case Sink.Kinesis => new KinesisSink(kinesisProvider, config, InputType.Bad).some
    case Sink.Stdouterr => new StdouterrSink(InputType.Bad).some
    case Sink.Test => None
  }

  // Iterate through an enriched CanonicalOutput object and tab separate
  // the fields to a string.
  def tabSeparateCanonicalOutput(output: EnrichedEvent): String = {
    output.getClass.getDeclaredFields
    .filter { field =>
      !DiscardedFields.contains(field.getName)
    }
    .map{ field =>
      field.setAccessible(true)
      Option(field.get(output)).getOrElse("")
    }.mkString("\t")
  }

  // Helper method to enrich an event.
  // TODO: this is a slightly odd design: it's a pure function if our
  // our sink is Test, but it's an impure function (with
  // storeCanonicalOutput side effect) for the other sinks. We should
  // break this into a pure function with an impure wrapper.
  def enrichEvent(binaryData: Array[Byte]): List[Option[String]] = {
    val canonicalInput: ValidatedMaybeCollectorPayload = ThriftLoader.toCollectorPayload(binaryData)
    val processedEvents: List[ValidationNel[String, EnrichedEvent]] = EtlPipeline.processEvents(
      enrichmentRegistry, s"kinesis-${generated.Settings.version}", System.currentTimeMillis.toString, canonicalInput)
    processedEvents.map(validatedMaybeEvent => {
      validatedMaybeEvent match {
        case Success(co) => {
          val ts = tabSeparateCanonicalOutput(co)
          for (s <- sink) {
            // TODO: pull this side effect into parent function
            s.storeEnrichedEvent(ts, co.user_ipaddress)
          }
          Some(ts)
        }
        case Failure(errors) => {
          for (s <- badSink) {
            val line = new String(Base64.encodeBase64(binaryData))
            s.storeEnrichedEvent(BadRow(line, errors).toCompactJson, "fail")
          }
          None
        }
      }
    })
  }

  // Initialize a Kinesis provider with the given credentials.
  private def createKinesisProvider(): AWSCredentialsProvider =  {
    val a = config.accessKey
    val s = config.secretKey
    if (isCpf(a) && isCpf(s)) {
        new ClasspathPropertiesFileCredentialsProvider()
    } else if (isCpf(a) || isCpf(s)) {
      throw new RuntimeException(
        "access-key and secret-key must both be set to 'cpf', or neither"
      )
    } else {
      new BasicAWSCredentialsProvider(
        new BasicAWSCredentials(a, s)
      )
    }
  }
  private def isCpf(key: String): Boolean = (key == "cpf")

  // Wrap BasicAWSCredential objects.
  class BasicAWSCredentialsProvider(basic: BasicAWSCredentials) extends
      AWSCredentialsProvider{
    @Override def getCredentials: AWSCredentials = basic
    @Override def refresh = {}
  }
}
