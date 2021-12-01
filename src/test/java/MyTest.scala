import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.junit.Test

@Test
class MyTest {

  @Test
  def test1() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = env.fromElements("Flink Batch", "batch demo", "demo", "demo")

    val ds = text.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    ds.print()
  }
}
