import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

def autoColFlatten(dfin : DataFrame):DataFrame= {
  val df=dfin.toDF(dfin.columns.map(x=>x.replace(" ","_")):_*)
  val  fields=df.schema.fields
  val fieldNames=fields.map(x=>x.name)
  val length=fields.length
  for (i <- 0 to fields.length-1){
      val field=fields(i)
      val allfieldtypes=field.dataType
      val fieldName=field.name
      allfieldtypes match {
           case arrayType: ArrayType =>
              val fieldNamesExcludingArray = fieldNames.filter(_!= fieldName)
              val fieldNamesAndExplode =  fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
              val explodeDF=df.selectExpr(fieldNamesAndExplode:_*)
              return autoColFlatten(explodeDF)
            case structType: StructType =>
              val childFieldnames= structType.fieldNames.map(childname => fieldName+"."+childname)
              val newfieldNames = fieldNames.filter(_!=fieldName) ++  childFieldnames
              val newcolumns=  newfieldNames.map(x=>col(x.toString()).as(x.toString().replace(".","")))
              val explodedf=df.select(newcolumns:_*)
              return autoColFlatten(explodedf)
           case _ =>
       }
  }
  df
}

val js="""{"accounting":[{"firstName":"John","lastName":"Doe","age":23},{"firstName":"Mary","lastName":"Smith","age":32}],"sales":[{"firstName":"Sally","lastName":"Green","age":27},{"firstName":"Jim","lastName":"Galley","age":41}]}"""

var df=spark.read.json(sc.parallelize(Seq(js)).map(x=>x.toString))
autoColFlatten(df).printSchema

val js="""{"accounting":[{"firstName":"John","lastName":"Doe","age":23},{"firstName":"Mary","lastName":"Smith","age":32}],"sales firm":[{"firstName":"Sally","lastName":"Green","age":27},{"firstName":"Jim","lastName":"Galley","age":41}]}"""

df=spark.read.json(sc.parallelize(Seq(js)).map(x=>x.toString))
autoColFlatten(df).printSchema

val js="""{"accounting":[{"firstName":"John","lastName":"Doe","age":23},{"firstName":"Mary","lastName":"Smith","age":32}],"sales":[{"first Name":"Sally","lastName":"Green","age":27},{"first Name":"Jim","lastName":"Galley","age":41}]}"""

df=spark.read.json(sc.parallelize(Seq(js)).map(x=>x.toString))
autoColFlatten(df).printSchema

