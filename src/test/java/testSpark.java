import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class testSpark {

    @Test
    public void test(){
        SparkSession spark = SparkSession.builder().appName("fdf").master("local[1]").getOrCreate();
        spark.sql("select 1,'asdf'").write().mode("overwrite").parquet("/Users/ikruglov/Temp/test.parquet");
        Dataset<Row> result = spark.read().parquet("/Users/ikruglov/Temp/test.parquet");
        result.show();
    }
}
