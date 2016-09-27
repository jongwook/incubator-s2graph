/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.storage.hbase

import java.lang.Integer.valueOf

import org.apache.commons.io.IOUtils
import org.hbase.async.GetRequest

import scala.collection.JavaConversions._

/** Forces loading our modified version of Asynchbase GetRequest and Scanner classes */
object AsynchbaseClassLoader {

  val classLoader = getClass.getClassLoader
  val defineClass = classOf[ClassLoader].getDeclaredMethod("defineClass", classOf[String], classOf[Array[Byte]], classOf[Int], classOf[Int])
  defineClass.setAccessible(true)

  def init(): Unit = load("GetRequest", "Scanner")

  /** loads Asynchbase classes from s2core's classpath */
  def load(classes: String*): Unit = {
    for (name <- classes) {
      val path = s"org/hbase/async/$name.class"
      val classname = s"org.hbase.async.$name"

      val resources = classLoader.getResources(path).toSeq

      resources.find(_.toString.contains("s2")) match {
        case Some(url) =>
          val stream = url.openStream()
          val bytes = try {
            IOUtils.toByteArray(stream)
          } finally {
            stream.close()
          }

          val clazz = defineClass.invoke(classLoader, classname, bytes, valueOf(0), valueOf(bytes.length))
        case None =>
          throw new ClassNotFoundException(s"Could not find the S2Graph version of $classname")
      }
    }
  }

}
