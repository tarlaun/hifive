/*
 * Copyright 2020 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.beast.geolite

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import edu.ucr.cs.bdlab.beast.geolite.Feature.{readType, readValue, writeType, writeValue}
import edu.ucr.cs.bdlab.beast.util.{BitArray, KryoInputToObjectInput, KryoOutputToObjectOutput}
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.{Calendar, SimpleTimeZone, TimeZone}
import scala.collection.mutable.ArrayBuffer

/**
 * A Row that contains a geometry
 * @param _values an initial list of values that might or might not contain a [[Geometry]]
 * @param _schema the schema of the given values or `null` to auto-detect the types from the values
 * @param _pixelCount auxiliary integer stored with the feature
 */
class Feature(private var _values: Array[Any],
              private var _schema: StructType,
              private var _pixelCount: Int = 0)
  extends IFeature with Externalizable with KryoSerializable {

  // ===== Auxiliary constructors (each calls the primary) =====

  /** Zero-arg for Java serialization/deserialization frameworks */
  def this() = this(null, null, 0)

  /** Java/Scala-friendly convenience: (Array[Any], StructType) */
  def this(values: Array[Any], schema: StructType) = this(values, schema, 0)

  // ===========================================================

  override def schema: StructType = _schema

  private var values: Array[Any] =
    if (_values == null) Array.empty[Any]
    else _values.map {
      case string: UTF8String     => string.toString
      case geom: UnsafeArrayData  => GeometryDataType.deserialize(geom)
      case other: Any             => other
      case _                      => null
    }

  if (_schema == null && values != null)
    _schema = Feature.inferSchema(values)

  // Optionally add pixelCount to schema/values
  def modify_schema(): Unit = {
    if (_schema != null && !_schema.fieldNames.contains("pixelCount")) {
      this._schema = this._schema.add("pixelCount", IntegerType, nullable = true)
      this._values = if (_values == null) Array[Any](pixelCount) else _values :+ pixelCount
    }
  }

  // Keep only the geometry attribute
  def removeNonGeometricAttributes(): Unit = {
    if (_schema == null) return
    val geometryIndexOpt = _schema.fields.zipWithIndex.find(_._1.dataType == GeometryDataType).map(_._2)
    geometryIndexOpt.foreach { geometryIndex =>
      _schema = StructType(Seq(_schema.fields(geometryIndex)))
      values = Array(values(geometryIndex))
      _values = Array(_values(geometryIndex))
    }
  }

  def setStringAttributesToNull(): Feature = {
    val modifiedValues =
      if (_values == null) Array.empty[Any]
      else _values.clone()

    if (_schema != null) {
      for (i <- _schema.fields.indices) {
        if (_schema.fields(i).dataType == StringType) modifiedValues(i) = null
      }
    }
    new Feature(modifiedValues, _schema)
  }

  override def fieldIndex(name: String): Int = schema.fieldIndex(name)

  // ---------- Java Externalizable / Kryo ----------

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeShort(length)
    if (length > 0) {
      // Attribute names
      for (field <- schema)
        out.writeUTF(if (field.name == null) "" else field.name)
      // Attribute types
      for (field <- schema)
        writeType(field.dataType, out)
      // Attribute exists (bit mask)
      val attributeExists = new BitArray(length)
      for (i <- 0 until length)
        attributeExists.set(i, !isNullAt(i))
      attributeExists.writeBitsMinimal(out)
      // Attribute values
      for (i <- 0 until length; if !isNullAt(i)) {
        val value = values(i)
        writeValue(out, value, schema(i).dataType)
      }
      out.writeInt(pixelCount)
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    val recordLength: Int = in.readShort()
    val attributeNames = new Array[String](recordLength)
    val attributeTypes = new Array[DataType](recordLength)
    for (i <- 0 until recordLength) attributeNames(i) = in.readUTF()
    for (i <- 0 until recordLength) attributeTypes(i) = readType(in)
    this._schema = StructType((0 until recordLength).map(i => StructField(attributeNames(i), attributeTypes(i))))
    val attributeExists = new BitArray(recordLength)
    attributeExists.readBitsMinimal(in)
    this.values = new Array[Any](recordLength)
    for (i <- 0 until recordLength; if attributeExists.get(i))
      values(i) = readValue(in, attributeTypes(i))
    pixelCount = in.readInt()
  }

  override def write(kryo: Kryo, out: Output): Unit =
    writeExternal(new KryoOutputToObjectOutput(kryo, out))

  override def read(kryo: Kryo, in: Input): Unit =
    readExternal(new KryoInputToObjectInput(kryo, in))

  // ---------- Row-like API ----------

  override def length: Int = if (values == null) 0 else values.length

  override def get(i: Int): Any = values(i)

  /** Make a copy of this row. */
  override def copy(): Feature =
    new Feature(_values.clone(), _schema.copy(), _pixelCount)

  /** To Spark InternalRow */
  def toInternalRow: InternalRow =
    new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(values.map {
      case string: String => UTF8String.fromString(string)
      case geom: Geometry => GeometryDataType.serialize(geom)
      case other: Any     => other
      case _              => null
    })

  // pixelCount getter/setter
  def pixelCount: Int = _pixelCount
  def pixelCount_=(value: Int): Unit = { _pixelCount = value }
}

object Feature {

  val UTC: TimeZone = new SimpleTimeZone(0, "UTC")

  /** Maps each data type to its ordinal number */
  val typeOrdinals: Map[DataType, Int] = Map(
    ByteType -> 0,
    ShortType -> 1,
    IntegerType -> 2,
    LongType -> 3,
    FloatType -> 4,
    DoubleType -> 5,
    StringType -> 6,
    BooleanType -> 7,
    GeometryDataType -> 8,
    DateType -> 9,
    TimestampType -> 10,
    MapType(BinaryType, BinaryType, valueContainsNull = true) -> 11,
    ArrayType(BinaryType) -> 12
  )

  /** Reverse map */
  val ordinalTypes: Map[Int, DataType] = typeOrdinals.map(kv => (kv._2, kv._1))

  def writeType(t: DataType, out: ObjectOutput): Unit = t match {
    case mp: MapType =>
      out.writeByte(11); writeType(mp.keyType, out); writeType(mp.valueType, out)
    case ap: ArrayType =>
      out.writeByte(12); writeType(ap.elementType, out)
    case _ =>
      out.writeByte(typeOrdinals.getOrElse(t, -1))
  }

  def readType(in: ObjectInput): DataType = {
    val typeOrdinal = in.readByte()
    if (typeOrdinal == 11) {
      val keyType: DataType = readType(in)
      val valueType: DataType = readType(in)
      MapType(keyType, valueType, valueContainsNull = true)
    } else if (typeOrdinal == 12) {
      val elementType: DataType = readType(in)
      ArrayType(elementType, containsNull = true)
    } else {
      ordinalTypes.getOrElse(typeOrdinal, BinaryType)
    }
  }

  def writeValue(out: ObjectOutput, value: Any, t: DataType): Unit = t match {
    case ByteType         => out.writeByte(value.asInstanceOf[Number].byteValue())
    case ShortType        => out.writeShort(value.asInstanceOf[Number].shortValue())
    case IntegerType      => out.writeInt(value.asInstanceOf[Number].intValue())
    case LongType         => out.writeLong(value.asInstanceOf[Number].longValue())
    case FloatType        => out.writeFloat(value.asInstanceOf[Number].floatValue())
    case DoubleType       => out.writeDouble(value.asInstanceOf[Number].doubleValue())
    case StringType       => out.writeUTF(value.asInstanceOf[String])
    case BooleanType      => out.writeBoolean(value.asInstanceOf[Boolean])
    case GeometryDataType => new GeometryWriter().write(value.asInstanceOf[Geometry], out, true)
    case mapType: MapType =>
      val map = value.asInstanceOf[Map[Any, Any]]
      out.writeInt(map.size)
      for ((k, v) <- map) {
        writeValue(out, k, mapType.keyType)
        writeValue(out, v, mapType.valueType)
      }
    case _ => out.writeObject(value)
  }

  def readValue(in: ObjectInput, t: DataType): Any = t match {
    case ByteType         => in.readByte()
    case ShortType        => in.readShort()
    case IntegerType      => in.readInt()
    case LongType         => in.readLong()
    case FloatType        => in.readFloat()
    case DoubleType       => in.readDouble()
    case StringType       => in.readUTF()
    case BooleanType      => in.readBoolean()
    case GeometryDataType => GeometryReader.DefaultInstance.parse(in)
    case mt: MapType =>
      val size = in.readInt()
      val entries = new Array[(Any, Any)](size)
      for (i <- 0 until size) {
        val key = readValue(in, mt.keyType)
        val value = readValue(in, mt.valueType)
        entries(i) = (key, value)
      }
      entries.toMap
    case _ => in.readObject()
  }

  /** Initialize the schema from the given parameters where the first field is always the geometry. */
  private def makeSchema(names: Array[String], types: Array[DataType], values: Array[Any]): StructType = {
    val numAttributes: Int =
      if (names != null) names.length
      else if (types != null) types.length
      else if (values != null) values.length
      else 0
    val fields = new Array[StructField](numAttributes + 1)
    fields(0) = StructField("g", GeometryDataType)
    for (i <- 0 until numAttributes) {
      val fieldType: DataType =
        if (types != null && types(i) != null) types(i)
        else if (values != null && values(i) != null) inferType(values(i))
        else if (values(i) == null) NullType
        else StringType
      val name: String = if (names == null) null else names(i)
      fields(i + 1) = StructField(name, fieldType)
    }
    StructType(fields)
  }

  protected def inferType(value: Any): DataType = value match {
    case null                                     => NullType
    case _: String                                => StringType
    case _: Integer | _: Int | _: Byte | _: Short => IntegerType
    case _: java.lang.Long | _: Long              => LongType
    case _: java.lang.Double | _: Double | _: Float => DoubleType
    case _: java.sql.Timestamp                    => TimestampType
    case _: java.sql.Date                         => DateType
    case _: java.lang.Boolean | _: Boolean        => BooleanType
    case _: Geometry                              => GeometryDataType
    case map: scala.collection.immutable.HashMap[Object, Object] =>
      val keyType: DataType = inferType(map.keys.head)
      val valueType: DataType = inferType(map.values.head)
      DataTypes.createMapType(keyType, valueType)
    case list: scala.collection.Seq[Object] =>
      DataTypes.createArrayType(if (list.isEmpty) BinaryType else inferType(list.head))
  }

  /** Create an array of values that contains the given geometry. */
  private def makeValuesArray(geometry: Geometry, types: Array[DataType], values: Array[Any]): Array[Any] = {
    val numAttributes = if (types != null) types.length else if (values != null) values.length else 0
    if (values != null && numAttributes == values.length) geometry +: values
    else {
      val retVal = new Array[Any](numAttributes + 1)
      retVal(0) = geometry
      if (values != null) System.arraycopy(values, 0, retVal, 1, values.length)
      retVal
    }
  }

  /** Infer schema from values (BinaryType for unknowns) */
  private def inferSchema(values: Array[Any]): StructType =
    StructType(values.zipWithIndex.map(vi => StructField(s"$$${vi._2}", detectType(vi._1))))

  /** Detect Spark SQL type of a value */
  private def detectType(value: Any): DataType = value match {
    case null                                   => BinaryType
    case _: Byte                                => ByteType
    case _: Short                               => ShortType
    case _: Int                                 => IntegerType
    case _: Long                                => LongType
    case _: Float                               => FloatType
    case _: Double                              => DoubleType
    case _: String                              => StringType
    case x: java.math.BigDecimal                => DecimalType(x.precision(), x.scale())
    case _: java.sql.Date | _: java.time.LocalDate       => DateType
    case _: java.sql.Timestamp | _: java.time.Instant    => TimestampType
    case _: Array[Byte]                         => BinaryType
    case r: Row                                 => r.schema
    case _: Geometry                            => GeometryDataType
    case m: Map[Any, Any] =>
      val keyType: DataType = if (m.isEmpty) StringType else detectType(m.head._1)
      val valueType: DataType = if (m.isEmpty) StringType else detectType(m.head._2)
      MapType(keyType, valueType, valueContainsNull = true)
    case _                                      => BinaryType
  }

  def create(row: Row, geometry: Geometry): Feature = create(row, geometry, 0)

  /**
   * Create a Feature from a row and geometry.
   * Replaces existing geometry if present, otherwise prepends it.
   */
  def create(row: Row, geometry: Geometry, pixel: Int = 0): Feature =
    if (row == null) {
      new Feature(Array(geometry), StructType(Seq(StructField("g", GeometryDataType))), pixel)
    } else {
      val rowValues: Array[Any] = Row.unapplySeq(row).get.toArray
      val rowSchema: StructType = if (row.schema != null) row.schema else inferSchema(rowValues)
      val iGeom: Int = rowSchema.indexWhere(_.dataType == GeometryDataType)
      if (iGeom == -1) {
        val values: Array[Any] = geometry +: rowValues
        val schema: Seq[StructField] = Seq(StructField("g", GeometryDataType)) ++ rowSchema
        new Feature(values, StructType(schema))
      } else {
        rowValues(iGeom) = geometry
        new Feature(rowValues, rowSchema, pixel)
      }
    }

  /** Concatenate a feature row with another row */
  def concat(feature: IFeature, row: Row): IFeature = {
    val values = Row.unapplySeq(feature).get ++ Row.unapplySeq(row).get
    val schema = feature.schema ++ row.schema
    new Feature(values.toArray, StructType(schema))
  }

  /** Append one attribute to a feature */
  def append(feature: IFeature, value: Any, name: String = null, dataType: DataType = null): IFeature = {
    val values: Seq[Any] = Row.unapplySeq(feature).get :+ value
    val schema: Seq[StructField] = feature.schema :+ StructField(name, if (dataType != null) dataType else detectType(value))
    new Feature(values.toArray, StructType(schema))
  }

  def create(geometry: Geometry, _names: Array[String], _types: Array[DataType], _values: Array[Any]): Feature =
    new Feature(Feature.makeValuesArray(geometry, _types, _values), Feature.makeSchema(_names, _types, _values))

  def createFeatureWithoutExtraG(geometry: Geometry,
                                 fieldNames: Array[String],
                                 fieldTypes: Array[DataType],
                                 values: Array[Any]): Feature = {
    val existingGeomIndex = fieldNames.indexOf("g")
    if (existingGeomIndex != -1 && geometry != null) {
      values(existingGeomIndex) = geometry
      new Feature(values, makeSchema(fieldNames, fieldTypes, values))
    } else {
      new Feature(values, makeSchema(fieldNames, fieldTypes, values))
    }
  }
}
