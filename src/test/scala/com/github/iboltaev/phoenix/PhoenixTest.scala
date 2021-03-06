package com.github.iboltaev.phoenix

import java.sql.DriverManager
import java.util.UUID

import org.apache.spark.phoenix.toolkit.io.SortedParquet
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class PhoenixTest extends AnyFlatSpec with Matchers {
  val schema = StructType(Seq(
    StructField("ID", IntegerType, true),
    StructField("NAME", StringType, true)
  ))

  it should "write sorted csv and select from phoenix" in {
    val seq = for {
      i <- Stream.from(0)
      j <- Stream.continually(UUID.randomUUID().toString).take(10)
      row = new GenericRowWithSchema(Array(i, j), schema)
    } yield row.asInstanceOf[Row]

    val sample = seq.take(1000)
    val shuffled = Random.shuffle(sample)

    val res = for {
      conn <- resource.managed(DriverManager.getConnection("jdbc:phoenix:localhost"))
      stmt <- resource.managed(conn.createStatement())
    } yield (conn, stmt)

    res.acquireAndGet { case (conn, stmt) =>
      stmt.executeUpdate("drop table if exists EMP")
      stmt.executeUpdate("CREATE TABLE EMP(id integer NOT  NULL, name varchar NOT NULL, descr varchar, CONSTRAINT pk PRIMARY KEY (id, name)) split ON ((3,'ha'), (7, 'qf'))")

      shuffled.grouped(1000).foreach { seq =>
        seq.foreach { row =>
          val Seq(id, name) = row.toSeq
          stmt.executeUpdate(s"UPSERT INTO EMP(id, name, descr) VALUES(${id.asInstanceOf[Int]}, '${name.asInstanceOf[String]}', 'description!!!')")
        }
        conn.commit()
      }
    }

    resource.managed(SparkSession.builder().master("local[*]").getOrCreate()).acquireAndGet { ss =>
      // select 17 keys
      val df = ss.createDataFrame(ss.sparkContext.parallelize(shuffled.take(17), 4), schema)
      SortedParquet.write(df, "/tmp/out")

      val opts = Map(
        "leftTable" -> "EMP",
        "rightParquet" -> "/tmp/out",
        "phoenixJoinerJdbc" -> "jdbc:phoenix:localhost",
        "phoenixJoinerZk" -> "localhost:2181",
        "phoenixJoinerKeySchemaJson" -> schema.json)

      val rd = ss.read.format("org.apache.spark.phoenix.toolkit.io.PhoenixJoinerProvider").options(opts).load()
      rd.show()

      rd.count().shouldBe(17)
    }
  }

  it should "read border keys" in {
    val res = for {
      conn <- resource.managed(DriverManager.getConnection("jdbc:phoenix:localhost"))
      stmt <- resource.managed(conn.createStatement())
    } yield (conn, stmt)

    res.acquireAndGet { case (conn, stmt) =>
      stmt.executeUpdate("drop table if exists EMP")
      stmt.executeUpdate("CREATE TABLE EMP(id integer NOT  NULL, name varchar NOT NULL, descr varchar, CONSTRAINT pk PRIMARY KEY (id, name)) split ON ((3,'ha'), (7, 'qf'))")

      stmt.executeUpdate(s"UPSERT INTO EMP(id,name,descr) VALUES(3, 'ha', 'descr')")
      stmt.executeUpdate(s"UPSERT INTO EMP(id,name,descr) VALUES(7, 'qf', 'descr2')")
      conn.commit()
    }

    val shuffled = Seq(Array(3, "ha"),  Array(7, "qf")).map { arr =>
      new GenericRowWithSchema(arr, schema).asInstanceOf[Row]
    }

    resource.managed(SparkSession.builder().master("local[*]").getOrCreate()).acquireAndGet { ss =>
      val df = ss.createDataFrame(ss.sparkContext.parallelize(shuffled, 4), schema)
      SortedParquet.write(df, "/tmp/out")

      val opts = Map(
        "leftTable" -> "EMP",
        "rightParquet" -> "/tmp/out",
        "phoenixJoinerJdbc" -> "jdbc:phoenix:localhost",
        "phoenixJoinerZk" -> "localhost:2181",
        "phoenixJoinerKeySchemaJson" -> schema.json)

      val rd = ss.read.format("org.apache.spark.phoenix.toolkit.io.PhoenixJoinerProvider").options(opts).load()
      rd.show()

      rd.count().shouldBe(2)
    }
  }
}
