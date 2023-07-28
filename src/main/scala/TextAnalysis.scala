import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.jsoup.Jsoup
import scalaj.http.Http

import scala.util.matching.Regex
object TextAnalysis extends App {

  val spark = SparkSession.builder().master("local").appName("Analysis").getOrCreate()

  val stopWordsData = spark.sparkContext.textFile("src/main/resources/StopWords/*.txt")
  val positiveWordsData=spark.sparkContext.textFile("src/main/resources/MasterDictionary/positive-words.txt")
  val negativeWordsData=spark.sparkContext.textFile("src/main/resources/MasterDictionary/negative-words.txt")
  val urlLinksDF = spark.read.format("com.crealytics.spark.excel").option("header","true").load("src/main/resources/Input.xlsx")
  val stopWords = spark.sparkContext.broadcast(stopWordsData.map(_.split(" ")(0)).map(_.toLowerCase).collect().toSet)
  val positiveWords = spark.sparkContext.broadcast(positiveWordsData.subtract(stopWordsData).collect().toSet)
  val negativeWords = spark.sparkContext.broadcast(negativeWordsData.subtract(stopWordsData).collect().toSet)
  val punctuations = Set('!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '[', '\\', ']', '^', '_', '`', '{', '|', '}', '~')

  def get_data(url: String): String = {
    try {
      val result = Http(url).asString
      if (result.isSuccess) {
        val c = result.body
        val soup = Jsoup.parse(c)
        var article_text = ""
        val all_p = soup.select("div.td-post-content").select("p")
        import scala.jdk.CollectionConverters._
        for (element <- all_p.asScala) {
          article_text += " " + element.text()
        }
        val title = soup.select("title").text()
        article_text = title + ". " + article_text
        article_text = article_text.linesIterator.map(_.trim).filter(_.nonEmpty).mkString(" ")
        article_text
      } else {
        println("HTTP Error: " + result.code + " " + result.statusLine)
        ""
      }
    } catch {
      case _: Exception => ""
    }
  }

  def count_syllables(word: String): Int = {
    val vowels = Set('a', 'e', 'i', 'o', 'u', 'y')
    var count = 0
    var prevChar: Char = ' '
    for (char <- word.toLowerCase) {
      if (vowels.contains(char) && !vowels.contains(prevChar)) {
        count += 1
      }
      prevChar = char
    }
    if (word.endsWith("es") || word.endsWith("ed")) {
      count -= 1
    }
    count
  }
  val pronounPattern: Regex = "\\b(I|we|my|ours|us)\\b".r

  def Analysis(text: String): Map[String, Double] = {
    val tokens = text.toLowerCase.split("\\s+")
    val tokens_without_pun = tokens.map(token => token.filterNot(punctuations.contains))
    val tokens_clean = tokens_without_pun.filterNot(stopWords.value.contains)
    val positive_score = tokens_clean.count(positiveWords.value.contains).toDouble
    val negative_score = tokens_clean.count(negativeWords.value.contains).toDouble
    val polarity_score = (positive_score - negative_score) / (positive_score + negative_score + 0.000001)
    val subjectivity_score = (positive_score + negative_score) / (tokens_clean.length + 0.000001)
    val word_count = tokens_without_pun.length
    val syllable_count_per_word = tokens_without_pun.map(count_syllables).sum / (word_count + 0.000001)
    val sentence_tokens = text.split("(?<=[.!?])")
    val sentence_count = sentence_tokens.length
    val avg_sentence_length = word_count / (sentence_count + 0.000001)
    val complex_word_count = tokens_without_pun.count(word => count_syllables(word) > 2).toDouble
    val percentage_complex_words = complex_word_count / (word_count + 0.000001)
    val fog_index = 0.4 * (avg_sentence_length + percentage_complex_words)
    val avg_words_per_sentence = word_count / (sentence_count + 0.000001)
    val personal_pronouns = pronounPattern.findAllIn(text).size.toDouble
    val total_word_length = tokens_without_pun.map(_.length).sum.toDouble
    val avg_word_length = total_word_length / (word_count + 0.000001)
    Map(
      "Positive Score" -> positive_score,
      "Negative Score" -> negative_score,
      "Polarity Score" -> polarity_score,
      "Subjectivity Score" -> subjectivity_score,
      "Word Count" -> word_count,
      "Syllable Count Per Word" -> syllable_count_per_word,
      "Complex Word Count" -> complex_word_count,
      "Avg Sentence Length" -> avg_sentence_length,
      "Percentage of Complex Words" -> percentage_complex_words,
      "Fog Index" -> fog_index,
      "Avg Words per Sentence" -> avg_words_per_sentence,
      "Personal Pronouns" -> personal_pronouns,
      "Avg Word Length" -> avg_word_length
    )
  }
  val getDataUDF = udf((url: String) => get_data(url))
  val analysisUDF = udf((text: String) => Analysis(text))
  val textDF = urlLinksDF.withColumn("text", getDataUDF(col("url")))
  textDF.cache()
  val resultDF = textDF.withColumn("analysis_result", analysisUDF(col("text")))
  val finalResultDF = resultDF
    .select(
      col("url_id"),
      col("url"),
      col("analysis_result")("Positive Score").as("Positive Score"),
      col("analysis_result")("Negative Score").as("Negative Score"),
      col("analysis_result")("Polarity Score").as("Polarity Score"),
      col("analysis_result")("Subjectivity Score").as("Subjectivity Score"),
      col("analysis_result")("Word Count").as("Word Count"),
      col("analysis_result")("Syllable Count Per Word").as("Syllable Count Per Word"),
      col("analysis_result")("Complex Word Count").as("Complex Word Count"),
      col("analysis_result")("Avg Sentence Length").as("Avg Sentence Length"),
      col("analysis_result")("Percentage of Complex Words").as("Percentage of Complex Words"),
      col("analysis_result")("Fog Index").as("Fog Index"),
      col("analysis_result")("Avg Words per Sentence").as("Avg Words per Sentence"),
      col("analysis_result")("Personal Pronouns").as("Personal Pronouns"),
      col("analysis_result")("Avg Word Length").as("Avg Word Length")
    )
 // finalResultDF.write.format("com.crealytics.spark.excel").option("header", "true").save("src/Output.xlsx")
 finalResultDF.show(false)

}
