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
import java.nio.charset.StandardCharsets

import net.bytebuddy.ByteBuddy
import net.bytebuddy.description.modifier.Visibility.PUBLIC
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.implementation.FieldAccessor
import net.bytebuddy.implementation.MethodDelegation.to
import net.bytebuddy.matcher.ElementMatchers._
import org.apache.commons.io.IOUtils
import org.hbase.async.GetRequestExtra.GetRequestExtra
import org.hbase.async.ScannerExtra.ScannerExtra
import org.hbase.async._
import org.objectweb.asm.Opcodes.{ACC_FINAL, ACC_PRIVATE, ACC_PROTECTED, ACC_PUBLIC}
import org.objectweb.asm._

import scala.collection.JavaConversions._

/**
  * Upon initialization, it loads a patched version of Asynchbase's GetRequest and Scanner class,
  * modified using ASM to make the classes non-final and their methods are all public,
  * so that ByteBuddy can create subclasses of them.
  *
  * This object has to be initialized before any access to (i.e. any classloading of) Asynchbase,
  * since the ClassLoader does not allow redefining already loaded classes unless we use instrumentation.
  */
object AsynchbasePatcher {

  /** invoking this method will force loading this class, thus triggering the patch mechanism below */
  def init(): Unit = {
    assert(getRequestClass.getMethod("setStoreLimit", classOf[Int]) != null)
    assert(getRequestClass.getMethod("setStoreOffset", classOf[Int]) != null)
    assert(scannerClass.getMethod("setRpcTimeout", classOf[Int]) != null)
  }

  /** instantiates a new GetRequest, patched to support storeLimit/storeOffset */
  def newGetRequest(table: Array[Byte], key: Array[Byte], family: Array[Byte]): GetRequestExtra = {
    getRequestClass.getConstructor(BA, BA, BA)
      .newInstance(table, key, family).asInstanceOf[GetRequestExtra]
  }

  /** instantiates a new GetRequest, patched to support storeLimit/storeOffset */
  def newGetRequest(table: Array[Byte], key: Array[Byte], family: Array[Byte], qualifier: Array[Byte]): GetRequestExtra = {
    getRequestClass.getConstructor(BA, BA, BA, BA)
      .newInstance(table, key, family, qualifier).asInstanceOf[GetRequestExtra]
  }

  /** instantiate a new Scanner, patched to support RPC timeout */
  def newScanner(client: HBaseClient, table: Array[Byte]): ScannerExtra = {
    val constructor = scannerClass.getConstructor(classOf[HBaseClient], BA)
    constructor.setAccessible(true)
    constructor.newInstance(client, table).asInstanceOf[ScannerExtra]
  }

  /** instantiate a new Scanner, patched to support RPC timeout */
  def newScanner(client: HBaseClient, table: String): ScannerExtra = {
    newScanner(client, table.getBytes(StandardCharsets.UTF_8))
  }


  private val BA = classOf[Array[Byte]]
  private val classLoader = getClass.getClassLoader
  private val defineClass = classOf[ClassLoader].getDeclaredMethod("defineClass", classOf[String], classOf[Array[Byte]], classOf[Int], classOf[Int])
  defineClass.setAccessible(true)

  /** loads Asynchbase classes from s2core's classpath
    * *MUST* be called before any access to those classes,
    * otherwise the classloading will fail with an "attempted duplicate class definition" error.
    **/
  private def loadClass(name: String): Class[_] = {
    classLoader.getResources(s"org/hbase/async/$name.class").toSeq.headOption match {
      case Some(url) =>
        val stream = url.openStream()
        val bytes = try { IOUtils.toByteArray(stream) } finally { stream.close() }

        // patch the bytecode so that the class is no longer final and the methods are all accessible
        val cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES)
        new ClassReader(bytes).accept(new ClassAdapter(cw) {
          override def visit(version: Int, access: Int, name: String, signature: String, superName: String, interfaces: Array[String]): Unit = {
            super.visit(version, access & ~ACC_FINAL, name, signature, superName, interfaces)
          }
          override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]): MethodVisitor = {
            super.visitMethod(access & ~ACC_PRIVATE & ~ACC_PROTECTED & ~ACC_FINAL | ACC_PUBLIC, name, desc, signature, exceptions)
          }
        }, 0)
        val patched = cw.toByteArray

        defineClass.invoke(classLoader, s"org.hbase.async.$name", patched, valueOf(0), valueOf(patched.length)).asInstanceOf[Class[_]]
      case None =>
        throw new ClassNotFoundException(s"Could not find Asynchbase class: $name")
    }
  }

  /** a java.lang.Class instance for the patched GetRequest class */
  private val getRequestClass: Class[_] = {
    new ByteBuddy()
      .subclass(loadClass("GetRequest"))
      .name("org.hbase.async.GetRequestEx")
      .implement(classOf[GetRequestExtra.IsGetRequest])
      .implement(classOf[GetRequestExtra.StoreLimitOffset]).intercept(FieldAccessor.ofBeanProperty())
      .defineField("storeLimit", classOf[Int], PUBLIC)
      .defineField("storeOffset", classOf[Int], PUBLIC)
      .method(named("serialize")).intercept(to(GetRequestExtra))
      .make.load(classLoader, ClassLoadingStrategy.Default.INJECTION).getLoaded
  }

  /** a java.lang.Class instance for the patched Scanner class */
  private val scannerClass = {
    new ByteBuddy()
      .subclass(loadClass("Scanner"))
      .name("org.hbase.async.ScannerEx")
      .implement(classOf[ScannerExtra.RpcTimeout]).intercept(FieldAccessor.ofBeanProperty())
      .defineField("rpcTimeout", classOf[Int], PUBLIC)
      .method(named("getNextRowsRequest")).intercept(to(ScannerExtra))
      .make.load(classLoader, ClassLoadingStrategy.Default.INJECTION).getLoaded
  }

}
