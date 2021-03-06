package org.apache.spark.phoenix.toolkit.io

import java.sql.{Connection, DriverManager}
import java.util.{Map => UtilMap, Set => UtilSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.parquet.hadoop.ParquetReader
import org.apache.phoenix.query.HBaseFactoryProvider
import org.apache.phoenix.schema.RowKeySchema
import org.apache.phoenix.schema.types._
import org.apache.phoenix.util.{ColumnInfo, PhoenixRuntime}
import org.apache.spark.executor.InputMetrics
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.util.Try

class PhoenixJoinerProvider extends TableProvider {
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: UtilMap[String, String]): PhoenixJoinerTable = {
    val leftTable = properties.get("leftTable")
    val rightParquet = properties.get("rightParquet")
    val jdbc = properties.get("phoenixJoinerJdbc")
    val zk = properties.get("phoenixJoinerZk")
    val keySchema = properties.get("phoenixJoinerKeySchemaJson")
    new PhoenixJoinerTable(leftTable, rightParquet, schema, keySchema, jdbc, zk)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val tableName = options.get("leftTable")
    val jdbc = options.get("phoenixJoinerJdbc")
    Utils.schemaOf(tableName, jdbc)
  }

  override def supportsExternalMetadata(): Boolean = {
    false
  }
}

class PhoenixJoinerTable(leftTable: String, rightParquet: String, schema: StructType, keySchema: String, jdbc: String, zk: String) extends SupportsRead {
  override val name: String = leftTable

  override def capabilities(): UtilSet[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def schema(): StructType = {
    schema
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new PhoenixJoinerScanBuilder(leftTable, rightParquet, schema, keySchema, jdbc, zk)
  }
}

class PhoenixJoinerScanBuilder(leftTable: String, rightParquet: String, schema: StructType, keySchema: String, jdbc: String, zk: String) extends ScanBuilder {
  override def build(): Scan = {
    new PhoenixJoinerScan(leftTable, rightParquet, schema, keySchema, jdbc, zk)
  }
}

class PhoenixJoinerScan(leftTable: String, rightParquet: String, schema: StructType, keySchema: String, jdbc: String, zk: String) extends Scan {
  override def description(): String = {
    leftTable + "/" + rightParquet
  }

  override def readSchema(): StructType = {
    schema
  }

  override def toBatch: Batch = {
    new PhoenixJoinBatch(leftTable, rightParquet, keySchema, jdbc, zk)
  }
}

class PhoenixJoinBatch(leftTable: String, rightParquet: String, keySchema: String, jdbc: String, zk: String) extends Batch {
  override def planInputPartitions(): Array[InputPartition] = {
    val partitions = Utils.regionStartKeys(leftTable, jdbc, zk)

    val result = partitions.map { case (host, startKey, endKey) =>
      PhoenixJoinPartition(host, Utils.setUtf8(startKey), Utils.setUtf8(endKey)).asInstanceOf[InputPartition]
    }.toArray

    result
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new PhoenixJoinReaderFactory(leftTable, rightParquet, keySchema, jdbc)
  }
}

case class PhoenixJoinPartition(host: String, startKey: IndexedSeq[AnyRef], endKey: IndexedSeq[AnyRef]) extends InputPartition {
  override def preferredLocations(): Array[String] = Array(host)
}

// Serializable
class PhoenixJoinReaderFactory(leftTable: String, rightParquet: String, keySchema: String, jdbc: String) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PhoenixJoinPartitionReader(leftTable, rightParquet, keySchema, jdbc, partition.asInstanceOf[PhoenixJoinPartition])
  }
}

class PhoenixJoinPartitionReader(leftTable: String, rightParquet: String, keySchemaJson: String, jdbc: String, partition: PhoenixJoinPartition) extends PartitionReader[InternalRow] {
  lazy val fs = FileSystem.get(new Configuration())

  lazy val keySchema = DataType.fromJson(keySchemaJson) match {
    case s: StructType => s
    case _ => throw new RuntimeException("could not parse key schema")
  }

  lazy val schema = Utils.schemaOf(leftTable, jdbc)
  lazy val filesToRead = SortedParquet.filesToRead(rightParquet, fs, keySchema, partition.startKey, partition.endKey)
  lazy val connection = DriverManager.getConnection(jdbc)

  var currentReader: ParquetReader[InternalRow] = null

  lazy val keyIt = SortedParquet.iterateFiles(filesToRead, keySchema, partition.startKey, partition.endKey, currentReader = _)
    .grouped(1000)
    .flatMap { seq =>
      val rows = Utils.select(connection, leftTable, schema, keySchema, new InputMetrics, seq)
      rows.toIterator
    }

  override def next(): Boolean = {
    keyIt.hasNext
  }

  override def close(): Unit = {
    if (currentReader != null) Try(currentReader.close())
    Try(connection.close())
    Try(fs.close())
  }

  override def get(): InternalRow = {
    val res = keyIt.next()
    res
  }
}


object Utils {
  // performs select of records by keys
  def select(conn: Connection, tableName: String, schema: StructType, keySchema: StructType, metrics: InputMetrics, keys: Seq[InternalRow]): IndexedSeq[InternalRow] = {
    val tuple = if (keySchema.size == 1) "?" else "(" + Stream.fill(keySchema.size)("?").mkString(",") + ")"
    val values = Stream.fill(keys.size)(tuple).mkString(",")
    val indexedKeySchema = keySchema.zipWithIndex
    resource.managed(conn.prepareStatement(s"SELECT * FROM $tableName WHERE (${keySchema.fieldNames.mkString(",")}) IN ($values)")).acquireAndGet { stmt =>
      for {
        (ir, i) <- keys.zipWithIndex
        (field, j) <- indexedKeySchema
      } {
        val obj = ir.get(j, field.dataType)
        val realObj = obj match {
          case s: UTF8String => s.toString
          case v => v
        }
        stmt.setObject(i * keySchema.size + j + 1, realObj)
      }

      resource.managed(stmt.executeQuery()).acquireAndGet { rs =>
        JdbcUtils.resultSetToSparkInternalRows(rs, schema, metrics).toIndexedSeq
      }
    }
  }

  // returns regions host, start and end keys
  def regionStartKeys(tableName: String, jdbc: String, zk: String): Seq[(String, IndexedSeq[AnyRef], IndexedSeq[AnyRef])] = {
    val conf = new Configuration()
    conf.set("zkUrl", zk)

    val res = for {
      conn <- resource.managed(HBaseFactoryProvider.getHConnectionFactory.createConnection(conf))
      pConn <- resource.managed(DriverManager.getConnection(jdbc))
      regionLocator <- resource.managed(conn.getRegionLocator(TableName.valueOf(tableName)))
    } yield (pConn, regionLocator)

    res.acquireAndGet { case (pConn, rl) =>
      val pTable = PhoenixRuntime.getTableNoCache(pConn, "EMP")
      val regs = rl.getAllRegionLocations.asScala
      val rkSchema = pTable.getRowKeySchema

      regs.map { reg =>
        val startKey = reg.getRegionInfo.getStartKey
        val endKey = reg.getRegionInfo.getEndKey

        (reg.getHostname, readKey(startKey, rkSchema), readKey(endKey, rkSchema))
      }
    }
  }

  def schemaOf(tableName: String, jdbc: String): StructType = {
    resource.managed(DriverManager.getConnection(jdbc)).acquireAndGet { conn =>
      val cols = PhoenixRuntime.generateColumnInfo(conn, tableName, null)
      StructType(phoenixSchemaToCatalystSchema(cols.asScala))
    }
  }

  private def readKey(bytes: Array[Byte], schema: RowKeySchema): IndexedSeq[AnyRef] = {
    val ibw = new ImmutableBytesWritable()
    val maxOffset = schema.iterator(bytes, ibw)

    (0 until schema.getMaxFields).map { pos =>
      schema.next(ibw, pos, maxOffset)
      schema.getField(pos).getDataType.toObject(ibw)
    }
  }

  def setUtf8(seq: IndexedSeq[AnyRef]): IndexedSeq[AnyRef] = seq.map {
    case s: String => UTF8String.fromString(s)
    // case ir: InternalRow => ???
    case v => v
  }

  private def phoenixSchemaToCatalystSchema(columnList: Seq[ColumnInfo]) = {
    columnList.map(ci => {
      val structType = phoenixTypeToCatalystType(ci)
      StructField(ci.getDisplayName, structType)
    })
  }

  // Lookup table for Phoenix types to Spark catalyst types
  private def phoenixTypeToCatalystType(columnInfo: ColumnInfo): DataType = columnInfo.getPDataType match {
    case t if t.isInstanceOf[PVarchar] || t.isInstanceOf[PChar] => StringType
    case t if t.isInstanceOf[PLong] || t.isInstanceOf[PUnsignedLong] => LongType
    case t if t.isInstanceOf[PInteger] || t.isInstanceOf[PUnsignedInt] => IntegerType
    case t if t.isInstanceOf[PSmallint] || t.isInstanceOf[PUnsignedSmallint] => ShortType
    case t if t.isInstanceOf[PTinyint] || t.isInstanceOf[PUnsignedTinyint] => ByteType
    case t if t.isInstanceOf[PFloat] || t.isInstanceOf[PUnsignedFloat] => FloatType
    case t if t.isInstanceOf[PDouble] || t.isInstanceOf[PUnsignedDouble] => DoubleType
    // Use Spark system default precision for now (explicit to work with < 1.5)
    case t if t.isInstanceOf[PDecimal] =>
      if (columnInfo.getPrecision == null || columnInfo.getPrecision < 0) DecimalType(38, 18) else DecimalType(columnInfo.getPrecision, columnInfo.getScale)
    case t if t.isInstanceOf[PTimestamp] || t.isInstanceOf[PUnsignedTimestamp] => TimestampType
    case t if t.isInstanceOf[PTime] || t.isInstanceOf[PUnsignedTime] => TimestampType
    case t if (t.isInstanceOf[PDate] || t.isInstanceOf[PUnsignedDate]) && !false => DateType
    case t if (t.isInstanceOf[PDate] || t.isInstanceOf[PUnsignedDate]) && false => TimestampType
    case t if t.isInstanceOf[PBoolean] => BooleanType
    case t if t.isInstanceOf[PVarbinary] || t.isInstanceOf[PBinary] => BinaryType
    case t if t.isInstanceOf[PIntegerArray] || t.isInstanceOf[PUnsignedIntArray] => ArrayType(IntegerType, containsNull = true)
    case t if t.isInstanceOf[PBooleanArray] => ArrayType(BooleanType, containsNull = true)
    case t if t.isInstanceOf[PVarcharArray] || t.isInstanceOf[PCharArray] => ArrayType(StringType, containsNull = true)
    case t if t.isInstanceOf[PVarbinaryArray] || t.isInstanceOf[PBinaryArray] => ArrayType(BinaryType, containsNull = true)
    case t if t.isInstanceOf[PLongArray] || t.isInstanceOf[PUnsignedLongArray] => ArrayType(LongType, containsNull = true)
    case t if t.isInstanceOf[PSmallintArray] || t.isInstanceOf[PUnsignedSmallintArray] => ArrayType(IntegerType, containsNull = true)
    case t if t.isInstanceOf[PTinyintArray] || t.isInstanceOf[PUnsignedTinyintArray] => ArrayType(ByteType, containsNull = true)
    case t if t.isInstanceOf[PFloatArray] || t.isInstanceOf[PUnsignedFloatArray] => ArrayType(FloatType, containsNull = true)
    case t if t.isInstanceOf[PDoubleArray] || t.isInstanceOf[PUnsignedDoubleArray] => ArrayType(DoubleType, containsNull = true)
    case t if t.isInstanceOf[PDecimalArray] => ArrayType(
      if (columnInfo.getPrecision == null || columnInfo.getPrecision < 0) DecimalType(38, 18) else DecimalType(columnInfo.getPrecision, columnInfo.getScale), containsNull = true)
    case t if t.isInstanceOf[PTimestampArray] || t.isInstanceOf[PUnsignedTimestampArray] => ArrayType(TimestampType, containsNull = true)
    case t if t.isInstanceOf[PDateArray] || t.isInstanceOf[PUnsignedDateArray] => ArrayType(TimestampType, containsNull = true)
    case t if t.isInstanceOf[PTimeArray] || t.isInstanceOf[PUnsignedTimeArray] => ArrayType(TimestampType, containsNull = true)
  }
}