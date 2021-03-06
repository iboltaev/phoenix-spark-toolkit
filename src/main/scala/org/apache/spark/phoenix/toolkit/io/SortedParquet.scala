package org.apache.spark.phoenix.toolkit.io

import java.io.ByteArrayOutputStream
import java.util.Base64

import com.univocity.parsers.csv.CsvWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.parquet.hadoop
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetOutputFormat, ParquetReader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVOptions, UnivocityParser}
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.Searching
import scala.collection.Searching.{Found, InsertionPoint}
import scala.jdk.CollectionConverters.seqAsJavaListConverter

object SortedParquet extends Serializable {
  implicit private def toIterator[A](ri: RemoteIterator[A]): Iterator[A] = new Iterator[A] {
    override def hasNext: Boolean = ri.hasNext

    override def next(): A = ri.next
  }

  // sorts dataframe and writes it to a sorted sequence of sorted parquet files, encoding start key in file name
  def write(df: DataFrame, path: String): Unit = {
    val schema = df.schema
    val cols = df.columns.map(df.col)
    val count = df.count()
    val inOneFile = Math.ceil(Math.pow(count, 0.67)).toInt
    val files = Math.ceil(count.toDouble / inOneFile).toInt
    // TODO: deal with partitions!
    val sorted = df.sort(cols: _*)
    // coalesce without shuffle flag preserves rdd/dataframe ordering; with shuffle flag - nope.
    val partitioned = if (sorted.rdd.getNumPartitions > files) sorted.coalesce(files) else sorted
    val jsonSchema = schema.json

    resource.managed(FileSystem.get(new Configuration())).acquireAndGet { fs =>
      val ri = fs.listFiles(new Path(path), false)
      ri.foreach(lfs => fs.delete(lfs.getPath, false))
      writeImpl(partitioned, path, inOneFile, jsonSchema)
    }
  }

  // selects files in 'path' directory to be read, taking into account records interval [startKey, endKey]
  def filesToRead(path: String, fs: FileSystem, schema: StructType, startKey: IndexedSeq[AnyRef], endKey: IndexedSeq[AnyRef]): IndexedSeq[String] = {
    implicit val ordering = RowOrdering.createNaturalAscendingOrdering(schema.fields.map(_.dataType).toSeq)
    val startIr = InternalRow.fromSeq(startKey)
    val endIr = InternalRow.fromSeq(endKey)
    val startNull = startKey.forall(_ == null)
    val endNull = endKey.forall(_ == null)

    val ri = fs.listFiles(new Path(path), false)
    val opts = new CSVOptions(Map.empty, false, "UTC")
    val parser = new UnivocityParser(schema, opts).parse

    val files = ri.map { lfs =>
      val name = lfs.getPath.getName
      val csv = new String(Base64.getDecoder.decode(name.replace(".parquet", "")), "utf-8")
      val opt = parser(csv).get.copy()
      (opt, lfs.getPath.toString)
    }.toIndexedSeq

    if (files.isEmpty) IndexedSeq.empty[String]
    else if (files.size == 1) {
      val (ir, path) = files.head
      if (!endNull && ordering.lteq(endIr, ir)) IndexedSeq.empty[String] else IndexedSeq(path)
    } else {
      val filesMap = files.toMap
      val filesSorted = files.map(_._1).sorted
      lazy val startIdx = Searching.search(filesSorted).search(startIr) match {
        case Found(idx) => idx
        case InsertionPoint(idx) => Math.max(0, idx - 1)
      }
      lazy val endIdx = Searching.search(filesSorted).search(endIr) match {
        case Found(idx) => idx + 1
        case InsertionPoint(idx) => idx
      }

      val res = if (startNull && endNull) {
        filesSorted.map(filesMap)
      } else if (startNull) {
        filesSorted.take(endIdx).map(filesMap)
      } else if (endNull) {
        filesSorted.drop(startIdx).map(filesMap)
      } else {
        filesSorted.slice(startIdx, endIdx).map(filesMap)
      }

      res
    }
  }

  // iterate through selected files and choose only records that falls in [startKey, endKey] interval
  def iterateFiles(files: IndexedSeq[String], schema: StructType, startKey: IndexedSeq[AnyRef], endKey: IndexedSeq[AnyRef], setReader: ParquetReader[InternalRow] => Unit): Iterator[InternalRow] = {
    implicit val ordering = RowOrdering.createNaturalAscendingOrdering(schema.fields.map(_.dataType).toSeq)
    val startIr = InternalRow.fromSeq(startKey)
    val endIr = InternalRow.fromSeq(endKey)
    val startNull = startKey.forall(_ == null)
    val endNull = endKey.forall(_ == null)

    files.iterator.map(makeParquetReader(_, schema)).flatMap { pr =>
      setReader(pr)
      Iterator.continually {
        val read = pr.read()
        if (read != null) (pr, Option(read.copy()))
        else (pr, None)
      }.takeWhile(_._2.isDefined) ++ Iterator((pr, Option.empty[InternalRow]))
    }.flatMap { case (pr, opt) =>
      if (opt.isDefined) opt
      else {
        pr.close()
        setReader(null)
        opt
      }
    }.filter { ir =>
      (startNull || ordering.lteq(startIr, ir)) && (endNull || ordering.lt(ir, endIr))
    }
  }

  def makeParquetReader(fileName: String, schema: StructType): ParquetReader[InternalRow] = {
    val conf = new Configuration()
    conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, schema.json)
    conf.set(SQLConf.PARQUET_BINARY_AS_STRING.key, "true")
    conf.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, "true")

    hadoop.ParquetReader.builder[InternalRow](new ParquetReadSupport(), new fs.Path(fileName)).withConf(conf).build()
  }

  private def writeImpl(sorted: DataFrame, path: String, inOneFile: Int, jsonSchema: String): Unit = {
    sorted.foreachPartition { it: Iterator[Row] =>
      it.grouped(inOneFile).foreach { seq =>
        if (seq.nonEmpty) {
          val key = seq.head
          val opts = new CSVOptions(Map.empty, false, "UTC")
          val bas = new ByteArrayOutputStream()
          val writer = new CsvWriter(bas, opts.asWriterSettings)

          writer.writeRow(key.toSeq.asJava)
          writer.flush()

          val schema = DataType.fromJson(jsonSchema) match {
            case s: StructType => s
            case _ => throw new RuntimeException("could not parse key schema")
          }

          val fileName = path + "/" + Base64.getEncoder.encodeToString(bas.toByteArray) + ".parquet"
          writeParquetFile(fileName, schema, seq.iterator)
        }
      }
    }
  }

  private implicit class CloseableRecordWriter(rw: RecordWriter[Void, InternalRow]) extends AutoCloseable {
    def write(row: InternalRow): Unit = rw.write(null, row)

    override def close(): Unit = rw.close(null)
  }

  private def makeRecordWriter(conf: Configuration, outputFileName: String): CloseableRecordWriter =
    new ParquetOutputFormat[InternalRow]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path =
        (context, extension) match {
          case (_, _) =>
            new Path(outputFileName)
        }
    }.getRecordWriter(conf, new Path(outputFileName), CompressionCodecName.GZIP)

  private def writeParquetFile(fileName: String, schema: StructType, it: Iterator[Row]) = {
    val conf = new Configuration()

    conf.set("parquet.write.support.class", "org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport")
    conf.set("org.apache.spark.sql.parquet.row.attributes", schema.json)
    conf.set("spark.sql.parquet.writeLegacyFormat", "false")
    conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    resource.managed(makeRecordWriter(conf, fileName)).acquireAndGet { writer =>
      it.map(r => InternalRow.fromSeq(r.toSeq.map {
        case s: String => UTF8String.fromString(s)
        case v => v
      })).foreach(writer.write)
    }
  }
}
