// Databricks notebook source
// MAGIC %md
// MAGIC # Data-Intensive Programming - Group assignment
// MAGIC
// MAGIC This is the **Scala** version of the assignment. Switch to the Python version, if you want to do the assignment in Python.
// MAGIC
// MAGIC In all tasks, add your solutions to the cells following the task instructions. You are free to add new cells if you want.<br>
// MAGIC The example outputs, and some additional hints are given in a separate notebook in the same folder as this one.
// MAGIC
// MAGIC Don't forget to **submit your solutions to Moodle** once your group is finished with the assignment.
// MAGIC
// MAGIC ## Basic tasks (compulsory)
// MAGIC
// MAGIC There are in total nine basic tasks that every group must implement in order to have an accepted assignment.
// MAGIC
// MAGIC The basic task 1 is a separate task, and it deals with video game sales data. The task asks you to do some basic aggregation operations with Spark data frames.
// MAGIC
// MAGIC The other basic coding tasks (basic tasks 2-8) are all related and deal with data from [https://figshare.com/collections/Soccer_match_event_dataset/4415000/5](https://figshare.com/collections/Soccer_match_event_dataset/4415000/5) that contains information about events in [football](https://en.wikipedia.org/wiki/Association_football) matches in five European leagues during the season 2017-18. The tasks ask you to calculate the results of the matches based on the given data as well as do some further calculations. Special knowledge about football or the leagues is not required, and the task instructions should be sufficient in order to gain enough context for the tasks.
// MAGIC
// MAGIC Finally, the basic task 9 asks some information on your assignment working process.
// MAGIC
// MAGIC ## Advanced tasks (optional)
// MAGIC
// MAGIC There are in total of four advanced tasks that can be done to gain some course points. Despite the name, the advanced tasks may or may not be harder than the basic tasks.
// MAGIC
// MAGIC The advanced task 1 asks you to do all the basic tasks in an optimized way. It is possible that you gain some points from this without directly trying by just implementing the basic tasks efficiently. Logic errors and other issues that cause the basic tasks to give wrong results will be taken into account in the grading of the first advanced task. A maximum of 2 points will be given based on advanced task 1.
// MAGIC
// MAGIC The other three advanced tasks are separate tasks and their implementation does not affect the grade given for the advanced task 1.<br>
// MAGIC Only two of the three available tasks will be graded and each graded task can provide a maximum of 2 points to the total.<br>
// MAGIC If you attempt all three tasks, clearly mark which task you want to be used in the grading. Otherwise, the grader will randomly pick two of the tasks and ignore the third.
// MAGIC
// MAGIC Advanced task 2 continues with the football data and contains further questions that are done with the help of some additional data.<br>
// MAGIC Advanced task 3 deals with some image data and the questions are mostly related to the colors of the pixels in the images.<br>
// MAGIC Advanced task 4 asks you to do some classification related machine learning tasks with Spark.
// MAGIC
// MAGIC It is possible to gain partial points from the advanced tasks. I.e., if you have not completed the task fully but have implemented some part of the task, you might gain some appropriate portion of the points from the task. Logic errors, very inefficient solutions, and other issues will be taken into account in the task grading.
// MAGIC
// MAGIC ## Assignment grading
// MAGIC
// MAGIC Failing to do the basic tasks, means failing the assignment and thus also failing the course!<br>
// MAGIC "A close enough" solutions might be accepted => even if you fail to do some parts of the basic tasks, submit your work to Moodle.
// MAGIC
// MAGIC Accepted assignment submissions will be graded from 0 to 6 points.
// MAGIC
// MAGIC The maximum grade that can be achieved by doing only the basic tasks is 2/6 points (through advanced task 1).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Short summary
// MAGIC
// MAGIC ##### Minimum requirements (points: 0-2 out of maximum of 6):
// MAGIC
// MAGIC - All basic tasks implemented (at least in "a close enough" manner)
// MAGIC - Moodle submission for the group
// MAGIC
// MAGIC ##### For those aiming for higher points (0-6):
// MAGIC
// MAGIC - All basic tasks implemented
// MAGIC - Optimized solutions for the basic tasks (advanced task 1) (0-2 points)
// MAGIC - Two of the other three advanced tasks (2-4) implemented
// MAGIC     - Clearly marked which of the two tasks should be graded
// MAGIC     - Each graded advanced task will give 0-2 points
// MAGIC - Moodle submission for the group

// COMMAND ----------

// import statements for the entire notebook
// add anything that is required here

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DateType, ArrayType, IntegerType, StringType, DoubleType, StructField, StructType}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 1 - Video game sales data
// MAGIC
// MAGIC The CSV file `assignment/sales/video_game_sales.csv` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains video game sales data (based on [https://www.kaggle.com/datasets/patkle/video-game-sales-data-from-vgchartzcom](https://www.kaggle.com/datasets/patkle/video-game-sales-data-from-vgchartzcom)).
// MAGIC
// MAGIC Load the data from the CSV file into a data frame. The column headers and the first few data lines should give sufficient information about the source dataset. The numbers in the sales columns are given in millions.
// MAGIC
// MAGIC Using the data, find answers to the following:
// MAGIC
// MAGIC - Which publisher has the highest total sales in video games in North America considering games released in years 2006-2015?
// MAGIC - How many titles in total for this publisher do not have sales data available for North America considering games released in years 2006-2015?
// MAGIC - Separating games released in different years and considering only this publisher and only games released in years 2006-2015, what are the total sales, in North America and globally, for each year?
// MAGIC     - I.e., what are the total sales (in North America and globally) for games released by this publisher in year 2006? And the same for year 2007? ...
// MAGIC

// COMMAND ----------

val videoGameSalesSchema = StructType(Array(
  StructField("title", StringType, nullable = true),
  StructField("publisher", StringType, nullable = true),
  StructField("developer", StringType, nullable = true),
  StructField("release_date", DateType, nullable = true),
  StructField("platform", StringType, nullable = true),
  StructField("total_sales", DoubleType, nullable = true),
  StructField("na_sales", DoubleType, nullable = true)
))

val salesDF: DataFrame = spark.read
  .option("header", "true")
  .option("sep", "|")
  .schema(videoGameSalesSchema)
  .csv("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv")
  .select("title", "publisher", "release_date", "total_sales", "na_sales")

val filteredByReleaseDateDF = salesDF
  .filter(col("release_date").between("2006-01-01", "2015-12-31"))
  .cache()

val aggregatedDF = filteredByReleaseDateDF
  .groupBy("publisher")
  .agg(sum("na_sales").as("total_na_sales"))

val bestNAPublisher: String = aggregatedDF
  .orderBy(desc("total_na_sales"))
  .limit(1)
  .select("publisher")
  .first()
  .getString(0)

val titlesWithMissingSalesData: Long = filteredByReleaseDateDF
  .filter(col("publisher") === bestNAPublisher && col("na_sales").isNull)
  .count()

val bestNAPublisherSales: DataFrame = filteredByReleaseDateDF
  .filter(col("publisher") === bestNAPublisher)
  .withColumn("year", year(col("release_date")))
  .groupBy("year")
  .agg(
    round(sum("na_sales"), 2).as("na_total"),
    round(sum("total_sales"), 2).as("global_total")
  )
  .orderBy("year")

println(s"The publisher with the highest total video game sales in North America is: '${bestNAPublisher}'")
println(s"The number of titles with missing sales data for North America: ${titlesWithMissingSalesData}")
println("Sales data for the publisher:")
bestNAPublisherSales.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 2 - Event data from football matches
// MAGIC
// MAGIC A parquet file in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) at folder `assignment/football/events.parquet` based on [https://figshare.com/collections/Soccer_match_event_dataset/4415000/5](https://figshare.com/collections/Soccer_match_event_dataset/4415000/5) contains information about events in [football](https://en.wikipedia.org/wiki/Association_football) matches during the season 2017-18 in five European top-level leagues: English Premier League, Italian Serie A, Spanish La Liga, German Bundesliga, and French Ligue 1.
// MAGIC
// MAGIC #### Background information
// MAGIC
// MAGIC In the considered leagues, a season is played in a double round-robin format where each team plays against all other teams twice. Once as a home team in their own stadium and once as an away team in the other team's stadium. A season usually starts in August and ends in May.
// MAGIC
// MAGIC Each league match consists of two halves of 45 minutes each. Each half runs continuously, meaning that the clock is not stopped when the ball is out of play. The referee of the match may add some additional time to each half based on game stoppages. \[[https://en.wikipedia.org/wiki/Association_football#90-minute_ordinary_time](https://en.wikipedia.org/wiki/Association_football#90-minute_ordinary_time)\]
// MAGIC
// MAGIC The team that scores more goals than their opponent wins the match.
// MAGIC
// MAGIC **Columns in the data**
// MAGIC
// MAGIC Each row in the given data represents an event in a specific match. An event can be, for example, a pass, a foul, a shot, or a save attempt.
// MAGIC
// MAGIC Simple explanations for the available columns. Not all of these will be needed in this assignment.
// MAGIC
// MAGIC | column name | column type | description |
// MAGIC | ----------- | ----------- | ----------- |
// MAGIC | competition | string | The name of the competition |
// MAGIC | season | string | The season the match was played |
// MAGIC | matchId | integer | A unique id for the match |
// MAGIC | eventId | integer | A unique id for the event |
// MAGIC | homeTeam | string | The name of the home team |
// MAGIC | awayTeam | string | The name of the away team |
// MAGIC | event | string | The main category for the event |
// MAGIC | subEvent | string | The subcategory for the event |
// MAGIC | eventTeam | string | The name of the team that initiated the event |
// MAGIC | eventPlayerId | integer | The id for the player who initiated the event |
// MAGIC | eventPeriod | string | `1H` for events in the first half, `2H` for events in the second half |
// MAGIC | eventTime | double | The event time in seconds counted from the start of the half |
// MAGIC | tags | array of strings | The descriptions of the tags associated with the event |
// MAGIC | startPosition | struct | The event start position given in `x` and `y` coordinates in range \[0,100\] |
// MAGIC | enPosition | struct | The event end position given in `x` and `y` coordinates in range \[0,100\] |
// MAGIC
// MAGIC The used event categories can be seen from `assignment/football/metadata/eventid2name.csv`.<br>
// MAGIC And all available tag descriptions from `assignment/football/metadata/tags2name.csv`.<br>
// MAGIC You don't need to access these files in the assignment, but they can provide context for the following basic tasks that will use the event data.
// MAGIC
// MAGIC #### The task
// MAGIC
// MAGIC In this task you should load the data with all the rows into a data frame. This data frame object will then be used in the following basic tasks 3-8.

// COMMAND ----------

val eventDataSchema = StructType(Array(
  StructField("competition", StringType, nullable = true),
  StructField("season", StringType, nullable = true),
  StructField("matchId", IntegerType, nullable = true),
  StructField("homeTeam", StringType, nullable = true),
  StructField("awayTeam", StringType, nullable = true),
  StructField("event", StringType, nullable = true),
  StructField("eventTeam", StringType, nullable = true),
  StructField("eventPlayerId", IntegerType, nullable = true),
  StructField("eventPeriod", StringType, nullable = true),
  StructField("eventTime", DoubleType, nullable = true),
  StructField("tags", ArrayType(StringType, containsNull = true), nullable = true),
))

val eventDF: DataFrame = spark.read
  .schema(eventDataSchema)
  .parquet("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/football/events.parquet")
  .cache()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 3 - Calculate match results
// MAGIC
// MAGIC Create a match data frame for all the matches included in the event data frame created in basic task 2.
// MAGIC
// MAGIC The resulting data frame should contain one row for each match and include the following columns:
// MAGIC
// MAGIC | column name   | column type | description |
// MAGIC | ------------- | ----------- | ----------- |
// MAGIC | matchId       | integer     | A unique id for the match |
// MAGIC | competition   | string      | The name of the competition |
// MAGIC | season        | string      | The season the match was played |
// MAGIC | homeTeam      | string      | The name of the home team |
// MAGIC | awayTeam      | string      | The name of the away team |
// MAGIC | homeTeamGoals | integer     | The number of goals scored by the home team |
// MAGIC | awayTeamGoals | integer     | The number of goals scored by the away team |
// MAGIC
// MAGIC The number of goals scored for each team should be determined by the available event data.<br>
// MAGIC There are two events related to each goal:
// MAGIC
// MAGIC - One event for the player that scored the goal. This includes possible own goals.
// MAGIC - One event for the goalkeeper that tried to stop the goal.
// MAGIC
// MAGIC You need to choose which types of events you are counting.<br>
// MAGIC If you count both of the event types mentioned above, you will get double the amount of actual goals.

// COMMAND ----------


val goalEventsDF = eventDF
  .select("matchId", "eventTeam", "homeTeam", "awayTeam", "tags", "event")
  .filter((col("event").notEqual("Save attempt")) && 
    (array_contains(col("tags"), "Goal") || array_contains(col("tags"), "Own goal")))

val goalsByMatchDF = goalEventsDF
  .withColumn("isOwnGoal", array_contains(col("tags"), "Own goal"))
  .withColumn(
    "scoringTeam",
    when(col("isOwnGoal"),
      when(col("eventTeam") === col("homeTeam"), col("awayTeam")).otherwise(col("homeTeam"))
      ).otherwise(col("eventTeam"))
  )
  .groupBy("matchId", "scoringTeam")
  .agg(count("*").as("goals"))

val matchesDF = eventDF
  .select(
    col("matchId").as("id"),
    col("competition"),
    col("season"),
    col("homeTeam"),
    col("awayTeam")
  )
  .distinct()

val matchDF: DataFrame = matchesDF
  .join(goalsByMatchDF.as("home"), 
        col("homeTeam") === col("home.scoringTeam") && col("id") === col("home.matchId"), 
        "left"
  )
  .join(goalsByMatchDF.as("away"), 
        col("awayTeam") === col("away.scoringTeam") && col("id") === col("away.matchId"), 
        "left"
  )
  .select(
    col("competition"),
    col("season"),
    col("homeTeam"),
    col("awayTeam"),
    coalesce(col("home.goals"), lit(0)).as("homeTeamGoals"),
    coalesce(col("away.goals"), lit(0)).as("awayTeamGoals")
  )
  .cache()


// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 4 - Calculate team points in a season
// MAGIC
// MAGIC Create a season data frame that uses the match data frame from the basic task 3 and contains aggregated seasonal results and statistics for all the teams in all leagues. While the used dataset only includes data from a single season for each league, the code should be written such that it would work even if the data would include matches from multiple seasons for each league.
// MAGIC
// MAGIC ###### Game result determination
// MAGIC
// MAGIC - Team wins the match if they score more goals than their opponent.
// MAGIC - The match is considered a draw if both teams score equal amount of goals.
// MAGIC - Team loses the match if they score fewer goals than their opponent.
// MAGIC
// MAGIC ###### Match point determination
// MAGIC
// MAGIC - The winning team gains 3 points from the match.
// MAGIC - Both teams gain 1 point from a drawn match.
// MAGIC - The losing team does not gain any points from the match.
// MAGIC
// MAGIC The resulting data frame should contain one row for each team per league and season. It should include the following columns:
// MAGIC
// MAGIC | column name    | column type | description |
// MAGIC | -------------- | ----------- | ----------- |
// MAGIC | competition    | string      | The name of the competition |
// MAGIC | season         | string      | The season |
// MAGIC | team           | string      | The name of the team |
// MAGIC | games          | integer     | The number of games the team played in the given season |
// MAGIC | wins           | integer     | The number of wins the team had in the given season |
// MAGIC | draws          | integer     | The number of draws the team had in the given season |
// MAGIC | losses         | integer     | The number of losses the team had in the given season |
// MAGIC | goalsScored    | integer     | The total number of goals the team scored in the given season |
// MAGIC | goalsConceded  | integer     | The total number of goals scored against the team in the given season |
// MAGIC | points         | integer     | The total number of points gained by the team in the given season |

// COMMAND ----------

def calculateTeamStats(matchDF: DataFrame, teamColumn: String, goalsScoredColumn: String, goalsConcededColumn: String): DataFrame = {
  matchDF
    .withColumn("team", col(teamColumn))
    .withColumn("goalsScored", col(goalsScoredColumn))
    .withColumn("goalsConceded", col(goalsConcededColumn))
    .withColumn("wins", when(col(goalsScoredColumn) > col(goalsConcededColumn), 1).otherwise(0))
    .withColumn("draws", when(col(goalsScoredColumn) === col(goalsConcededColumn), 1).otherwise(0))
    .withColumn("losses", when(col(goalsScoredColumn) < col(goalsConcededColumn), 1).otherwise(0))
    .withColumn("points", col("wins") * 3 + col("draws"))
    .select("competition", "season", "team", "goalsScored", "goalsConceded", "wins", "draws", "losses", "points")
}

val homeTeamStatsDF = calculateTeamStats(matchDF, "homeTeam", "homeTeamGoals", "awayTeamGoals")
val awayTeamStatsDF = calculateTeamStats(matchDF, "awayTeam", "awayTeamGoals", "homeTeamGoals")

val seasonDF: DataFrame = homeTeamStatsDF.union(awayTeamStatsDF)
  .groupBy("competition", "season", "team")
  .agg(
    count("*").as("games"),
    sum("wins").as("wins"),
    sum("draws").as("draws"),
    sum("losses").as("losses"),
    sum("goalsScored").as("goalsScored"),
    sum("goalsConceded").as("goalsConceded"),
    sum("points").as("points")
  )

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 5 - English Premier League table
// MAGIC
// MAGIC Using the season data frame from basic task 4 calculate the final league table for `English Premier League` in season `2017-2018`.
// MAGIC
// MAGIC The result should be given as data frame which is ordered by the team's classification for the season.
// MAGIC
// MAGIC A team is classified higher than the other team if one of the following is true:
// MAGIC
// MAGIC - The team has a higher number of total points than the other team
// MAGIC - The team has an equal number of points, but have a better goal difference than the other team
// MAGIC - The team has an equal number of points and goal difference, but have more goals scored in total than the other team
// MAGIC
// MAGIC Goal difference is the difference between the number of goals scored for and against the team.
// MAGIC
// MAGIC The resulting data frame should contain one row for each team.<br>
// MAGIC It should include the following columns (several columns renamed trying to match the [league table in Wikipedia](https://en.wikipedia.org/wiki/2017%E2%80%9318_Premier_League#League_table)):
// MAGIC
// MAGIC | column name | column type | description |
// MAGIC | ----------- | ----------- | ----------- |
// MAGIC | Pos         | integer     | The classification of the team |
// MAGIC | Team        | string      | The name of the team |
// MAGIC | Pld         | integer     | The number of games played |
// MAGIC | W           | integer     | The number of wins |
// MAGIC | D           | integer     | The number of draws |
// MAGIC | L           | integer     | The number of losses |
// MAGIC | GF          | integer     | The total number of goals scored by the team |
// MAGIC | GA          | integer     | The total number of goals scored against the team |
// MAGIC | GD          | string      | The goal difference |
// MAGIC | Pts         | integer     | The total number of points gained by the team |
// MAGIC
// MAGIC The goal difference should be given as a string with an added `+` at the beginning if the difference is positive, similarly to the table in the linked Wikipedia article.

// COMMAND ----------

val seasons2017_2018DF = seasonDF
  .filter(col("season") === "2017-2018")
  .cache()

val premierLeague2017_2018DF = seasons2017_2018DF
  .filter(col("competition") === "English Premier League")

val goalDifferenceDF = premierLeague2017_2018DF
  .withColumn("GD",
    format_string("%+d", col("goalsScored") - col("goalsConceded"))
  )
  .orderBy(
    desc("points"),
    (col("goalsScored") - col("goalsConceded")).desc,
    desc("goalsScored")
  )

val englandDF: DataFrame = goalDifferenceDF
  .withColumn("Pos", monotonically_increasing_id() + 1)
  .select(
    col("Pos"),
    col("team").as("Team"),
    col("games").as("Pld"),
    col("wins").as("W"),
    col("draws").as("D"),
    col("losses").as("L"),
    col("goalsScored").as("GF"),
    col("goalsConceded").as("GA"),
    col("GD"),
    col("points").as("Pts")
  )

println("English Premier League table for season 2017-2018")
englandDF.show(20, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic task 6: Calculate the number of passes
// MAGIC
// MAGIC This task involves going back to the event data frame and counting the number of passes each team made in each match. A pass is considered successful if it is marked as `Accurate`.
// MAGIC
// MAGIC Using the event data frame from basic task 2, calculate the total number of passes as well as the total number of successful passes for each team in each match.<br>
// MAGIC The resulting data frame should contain one row for each team in each match, i.e., two rows for each match. It should include the following columns:
// MAGIC
// MAGIC | column name | column type | description |
// MAGIC | ----------- | ----------- | ----------- |
// MAGIC | matchId     | integer     | A unique id for the match |
// MAGIC | competition | string      | The name of the competition |
// MAGIC | season      | string      | The season |
// MAGIC | team        | string      | The name of the team |
// MAGIC | totalPasses | integer     | The total number of passes the team attempted in the match |
// MAGIC | successfulPasses | integer | The total number of successful passes made by the team in the match |
// MAGIC
// MAGIC You can assume that each team had at least one pass attempt in each match they played.

// COMMAND ----------

val passEventsDF = eventDF
  .select("matchId", "competition", "season", "eventTeam", "event", "tags")
  .filter(col("event").equalTo("Pass"))
  .withColumn("isSuccessful", array_contains(col("tags"), "Accurate"))

val matchPassDF: DataFrame = passEventsDF
  .groupBy("matchId", "competition", "season", "eventTeam")
  .agg(
    count("*").as("totalPasses"),
    sum(when(col("isSuccessful"), 1).otherwise(0)).as("successfulPasses")
  )
  .withColumnRenamed("eventTeam", "team")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 7: Teams with the worst passes
// MAGIC
// MAGIC Using the match pass data frame from basic task 6 find the teams with the lowest average ratio for successful passes over the season `2017-2018` for each league.
// MAGIC
// MAGIC The ratio for successful passes over a single match is the number of successful passes divided by the number of total passes.<br>
// MAGIC The average ratio over the season is the average of the single match ratios.
// MAGIC
// MAGIC Give the result as a data frame that has one row for each league-team pair with the following columns:
// MAGIC
// MAGIC | column name | column type | description |
// MAGIC | ----------- | ----------- | ----------- |
// MAGIC | competition | string      | The name of the competition |
// MAGIC | team        | string      | The name of the team |
// MAGIC | passSuccessRatio | double | The average ratio for successful passes over the season given as percentages rounded to two decimals |
// MAGIC
// MAGIC Order the data frame so that the team with the lowest ratio for passes is given first.

// COMMAND ----------

val passSuccessRatioDF: DataFrame = matchPassDF
  .filter(col("season") === "2017-2018")
  .withColumn("passSuccessRatio", (col("successfulPasses") / col("totalPasses")) * 100)
  .groupBy("competition", "team")
  .agg(round(avg("passSuccessRatio"), 2).as("passSuccessRatio"))
  .cache()

val lowestPassSuccessRatioDF: DataFrame = passSuccessRatioDF
  .withColumn("row", row_number().over(
      Window.partitionBy("competition").orderBy("passSuccessRatio")
  ))
  .filter(col("row") === 1)
  .select("competition", "team", "passSuccessRatio")
  .orderBy("passSuccessRatio")

println("The teams with the lowest ratios for successful passes for each league in season 2017-2018:")
lowestPassSuccessRatioDF.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic task 8: The best teams
// MAGIC
// MAGIC For this task the best teams are determined by having the highest point average per match.
// MAGIC
// MAGIC Using the data frames created in the previous tasks find the two best teams from each league in season `2017-2018` with their full statistics.
// MAGIC
// MAGIC Give the result as a data frame with the following columns:
// MAGIC
// MAGIC | column name | column type | description |
// MAGIC | ----------- | ----------- | ----------- |
// MAGIC | Team        | string      | The name of the team |
// MAGIC | League      | string      | The name of the league |
// MAGIC | Pos         | integer     | The classification of the team within their league |
// MAGIC | Pld         | integer     | The number of games played |
// MAGIC | W           | integer     | The number of wins |
// MAGIC | D           | integer     | The number of draws |
// MAGIC | L           | integer     | The number of losses |
// MAGIC | GF          | integer     | The total number of goals scored by the team |
// MAGIC | GA          | integer     | The total number of goals scored against the team |
// MAGIC | GD          | string      | The goal difference |
// MAGIC | Pts         | integer     | The total number of points gained by the team |
// MAGIC | Avg         | double      | The average points per match gained by the team |
// MAGIC | PassRatio   | double      | The average ratio for successful passes over the season given as percentages rounded to two decimals |
// MAGIC
// MAGIC Order the data frame so that the team with the highest point average per match is given first.

// COMMAND ----------


val teamStatsDF = seasons2017_2018DF
  .withColumn("GD",
    format_string("%+d", col("goalsScored") - col("goalsConceded"))
  )
  .withColumn("Avg", round(col("points") / col("games"), 2))

val passStatsDF = passSuccessRatioDF
  .withColumnRenamed("passSuccessRatio", "PassRatio")

val fullStatsDF = teamStatsDF
  .join(passStatsDF, Seq("competition", "team"), "left")
  .withColumnRenamed("competition", "League")
  .withColumnRenamed("team", "Team")

val leagueRanking = Window.partitionBy("League").orderBy(col("Avg").desc)
val rankedDF = fullStatsDF.withColumn("Pos", row_number().over(leagueRanking))

val bestDF: DataFrame = rankedDF
  .filter(col("Pos") <= 2)
  .select(
    col("Team"),
    col("League"),
    col("Pos"),
    col("games").as("Pld"),
    col("wins").as("W"),
    col("draws").as("D"),
    col("losses").as("L"),
    col("goalsScored").as("GF"),
    col("goalsConceded").as("GA"),
    col("GD"),
    col("points").as("Pts"),
    col("Avg"),
    col("PassRatio"),
  )
  .orderBy(col("Avg").desc)

println("The top 2 teams for each league in season 2017-2018")
bestDF.show(10, false)

// COMMAND ----------


// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced tasks
// MAGIC
// MAGIC The implementation of the basic tasks is compulsory for every group.
// MAGIC
// MAGIC Doing the following advanced tasks you can gain course points which can help in getting a better grade from the course.<br>
// MAGIC Partial solutions can give partial points.
// MAGIC
// MAGIC The advanced task 1 will be considered in the grading for every group based on their solutions for the basic tasks.
// MAGIC
// MAGIC The advanced tasks 2, 3, and 4 are separate tasks. The solutions used in these other advanced tasks do not affect the grading of advanced task 1. Instead, a good use of optimized methods can positively influence the grading of each specific task, while very non-optimized solutions can have a negative effect on the task grade.
// MAGIC
// MAGIC While you can attempt all three tasks (advanced tasks 2-4), only two of them will be graded and contribute towards the course grade.<br>
// MAGIC Mark in the following cell which tasks you want to be graded and which should be ignored.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### If you did the advanced tasks 2-4, mark here which of the two should be considered in grading:
// MAGIC
// MAGIC - Advanced task 2 should be graded: Yes
// MAGIC - Advanced task 3 should be graded: Yes
// MAGIC - Advanced task 4 should be graded: No

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced Task 1 - Optimized and correct solutions to the basic tasks (2 points)
// MAGIC
// MAGIC Use the tools Spark offers effectively and avoid unnecessary operations in the code for the basic tasks.
// MAGIC
// MAGIC A couple of things to consider (**not** even close to a complete list):
// MAGIC
// MAGIC - Consider using explicit schemas when dealing with CSV data sources.
// MAGIC - Consider only including those columns from a data source that are actually needed.
// MAGIC - Filter unnecessary rows whenever possible to get smaller datasets.
// MAGIC - Avoid collect or similar expensive operations for large datasets.
// MAGIC - Consider using explicit caching if some data frame is used repeatedly.
// MAGIC - Avoid unnecessary shuffling (for example sorting) operations.
// MAGIC - Avoid unnecessary actions (count, etc.) that are not needed for the task.
// MAGIC
// MAGIC In addition to the effectiveness of your solutions, the correctness of the solution logic will be taken into account when determining the grade for this advanced task 1.
// MAGIC "A close enough" solution with some logic fails might be enough to have an accepted group assignment, but those failings might lower the score for this task.
// MAGIC
// MAGIC It is okay to have your own test code that would fall into category of "ineffective usage" or "unnecessary operations" while doing the assignment tasks. However, for the final Moodle submission you should comment out or delete such code (and test that you have not broken anything when doing the final modifications).
// MAGIC
// MAGIC Note, that you should not do the basic tasks again for this additional task, but instead modify your basic task code with more efficient versions.
// MAGIC
// MAGIC You can create a text cell below this one and describe what optimizations you have done. This might help the grader to better recognize how skilled your work with the basic tasks has been.

// COMMAND ----------

// MAGIC %md
// MAGIC Optimizations:
// MAGIC
// MAGIC Using explicit schemas when dealing with CSV data sources.
// MAGIC
// MAGIC Only including those columns from a data source that are actually needed.
// MAGIC
// MAGIC Filtering unnecessary rows whenever possible to get smaller datasets.
// MAGIC
// MAGIC Using explicit caching if some data frame is used repeatedly.
// MAGIC
// MAGIC Avoiding unnecessary shuffling (for example sorting) operations.
// MAGIC
// MAGIC Plus other optimizations like avoiding unnecessary actions and expensive operations etc.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced Task 2 - Further tasks with football data (2 points)
// MAGIC
// MAGIC This advanced task continues with football event data from the basic tasks. In addition, there are two further related datasets that are used in this task.
// MAGIC
// MAGIC A Parquet file at folder `assignment/football/matches.parquet` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains information about which players were involved on each match including information on the substitutions made during the match.
// MAGIC
// MAGIC Another Parquet file at folder `assignment/football/players.parquet` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains information about the player names, default roles when playing, and their birth areas.
// MAGIC
// MAGIC #### Columns in the additional data
// MAGIC
// MAGIC The match dataset (`assignment/football/matches.parquet`) has one row for each match and each row has the following columns:
// MAGIC
// MAGIC | column name  | column type | description |
// MAGIC | ------------ | ----------- | ----------- |
// MAGIC | matchId      | integer     | A unique id for the match |
// MAGIC | competition  | string      | The name of the league |
// MAGIC | season       | string      | The season the match was played |
// MAGIC | roundId      | integer     | A unique id for the round in the competition |
// MAGIC | gameWeek     | integer     | The gameWeek of the match |
// MAGIC | date         | date        | The date the match was played |
// MAGIC | status       | string      | The status of the match, `Played` if the match has been played |
// MAGIC | homeTeamData | struct      | The home team data, see the table below for the attributes in the struct |
// MAGIC | awayTeamData | struct      | The away team data, see the table below for the attributes in the struct |
// MAGIC | referees     | struct      | The referees for the match |
// MAGIC
// MAGIC Both team data columns have the following inner structure:
// MAGIC
// MAGIC | column name  | column type | description |
// MAGIC | ------------ | ----------- | ----------- |
// MAGIC | team         | string      | The name of the team |
// MAGIC | coachId      | integer     | A unique id for the coach of the team |
// MAGIC | lineup       | array of integers | A list of the player ids who start the match on the field for the team |
// MAGIC | bench        | array of integers | A list of the player ids who start the match on the bench, i.e., the reserve players for the team |
// MAGIC | substitution1 | struct     | The first substitution the team made in the match, see the table below for the attributes in the struct |
// MAGIC | substitution2 | struct     | The second substitution the team made in the match, see the table below for the attributes in the struct |
// MAGIC | substitution3 | struct     | The third substitution the team made in the match, see the table below for the attributes in the struct |
// MAGIC
// MAGIC Each substitution structs have the following inner structure:
// MAGIC | column name  | column type | description |
// MAGIC | ------------ | ----------- | ----------- |
// MAGIC | playerIn     | integer     | The id for the player who was substituted from the bench into the field, i.e., this player started playing after this substitution |
// MAGIC | playerOut    | integer     | The id for the player who was substituted from the field to the bench, i.e., this player stopped playing after this substitution |
// MAGIC | minute       | integer     | The minute from the start of the match the substitution was made.<br>Values of 45 or less indicate that the substitution was made in the first half of the match,<br>and values larger than 45 indicate that the substitution was made on the second half of the match. |
// MAGIC
// MAGIC The player dataset (`assignment/football/players.parquet`) has the following columns:
// MAGIC
// MAGIC | column name  | column type | description |
// MAGIC | ------------ | ----------- | ----------- |
// MAGIC | playerId     | integer     | A unique id for the player |
// MAGIC | firstName    | string      | The first name of the player |
// MAGIC | lastName     | string      | The last name of the player |
// MAGIC | birthArea    | string      | The birth area (nation or similar) of the player |
// MAGIC | role         | string      | The main role of the player, either `Goalkeeper`, `Defender`, `Midfielder`, or `Forward` |
// MAGIC | foot         | string      | The stronger foot of the player |
// MAGIC
// MAGIC #### Background information
// MAGIC
// MAGIC In a football match both teams have 11 players on the playing field or pitch at the start of the match. Each team also have some number of reserve players on the bench at the start of the match. The teams can make up to three substitution during the match where they switch one of the players on the field to a reserve player. (Currently, more substitutions are allowed, but at the time when the data is from, three substitutions were the maximum.) Any player starting the match as a reserve and who is not substituted to the field during the match does not play any minutes and are not considered involved in the match.
// MAGIC
// MAGIC For this task the length of each match should be estimated with the following procedure:
// MAGIC
// MAGIC - Only the additional time added to the second half of the match should be considered. I.e., the length of the first half is always considered to be 45 minutes.
// MAGIC - The length of the second half is to be considered as the last event of the half rounded upwards towards the nearest minute.
// MAGIC     - I.e., if the last event of the second half happens at 2845 seconds (=47.4 minutes) from the start of the half, the length of the half should be considered as 48 minutes. And thus, the full length of the entire match as 93 minutes.
// MAGIC
// MAGIC A personal plus-minus statistics for each player can be calculated using the following information:
// MAGIC
// MAGIC - If a goal was scored by the player's team when the player was on the field, `add 1`
// MAGIC - If a goal was scored by the opponent's team when the player was on the field, `subtract 1`
// MAGIC - If a goal was scored when the player was a reserve on the bench, `no change`
// MAGIC - For any event that is not a goal, or is in a match that the player was not involved in, `no change`
// MAGIC - Any substitutions is considered to be done at the start of the given minute.
// MAGIC     - I.e., if the player is substituted from the bench to the field at minute 80 (minute 35 on the second half), they were considered to be on the pitch from second 2100.0 on the 2nd half of the match.
// MAGIC
// MAGIC ### Tasks
// MAGIC
// MAGIC The target of the task is to use the football event data and the additional datasets to determine the following:
// MAGIC
// MAGIC - The players with the most total minutes played in season 2017-2018 for each player role
// MAGIC     - I.e., the player in Goalkeeper role who has played the longest time across all included leagues. And the same for the other player roles (Defender, Midfielder, and Forward)
// MAGIC     - Give the result as a data frame that has the following columns:
// MAGIC         - `role`: the player role
// MAGIC         - `player`: the full name of the player, i.e., the first name combined with the last name
// MAGIC         - `birthArea`: the birth area of the player
// MAGIC         - `minutes`: the total minutes the player played during season 2017-2018
// MAGIC - The players with higher than `+65` for the total plus-minus statistics in season 2017-2018
// MAGIC     - Give the result as a data frame that has the following columns:
// MAGIC         - `player`: the full name of the player, i.e., the first name combined with the last name
// MAGIC         - `birthArea`: the birth area of the player
// MAGIC         - `role`: the player role
// MAGIC         - `plusMinus`: the total plus-minus statistics for the player during season 2017-2018
// MAGIC
// MAGIC It is advisable to work towards the target results using several intermediate steps.

// COMMAND ----------

val matchesInfoDF: DataFrame = spark.read
  .parquet("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/football/matches.parquet")
  .select("matchId", "season", "homeTeamData", "awayTeamData")

val playersInfoDF: DataFrame = spark.read
  .parquet("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/football/players.parquet")
  .select("playerId", "firstName", "lastName", "birthArea", "role")
  .cache()

val events2017_2018DF = eventDF
  .filter(col("season") === "2017-2018")
  .cache()

val matches2017_2018DF = matchesInfoDF
  .filter(col("season") === "2017-2018")

val matchLengthsDF = events2017_2018DF
  .filter(col("eventPeriod") === "2H")
  .groupBy(col("matchId"))
  .agg((ceil(max("eventTime") / 60) + 45).as("matchLength"))

val combinedMatchesInfoDF = matches2017_2018DF
  .join(matchLengthsDF, "matchId")
  .select(
    col("matchId"),
    col("homeTeamData.team").as("homeTeam"),
    col("awayTeamData.team").as("awayTeam"),
    col("homeTeamData.lineup").as("homeLineup"),
    col("homeTeamData.bench").as("homeBench"),
    col("homeTeamData.substitution1").as("homeSub1"),
    col("homeTeamData.substitution2").as("homeSub2"),
    col("homeTeamData.substitution3").as("homeSub3"),
    col("awayTeamData.lineup").as("awayLineup"),
    col("awayTeamData.bench").as("awayBench"),
    col("awayTeamData.substitution1").as("awaySub1"),
    col("awayTeamData.substitution2").as("awaySub2"),
    col("awayTeamData.substitution3").as("awaySub3"),
    col("matchLength")
  )
  .cache()

val startingPlayersDF = combinedMatchesInfoDF
  .withColumn("lineup", array_union(col("homeLineup"), col("awayLineup")))
  .withColumn("playerId", explode(col("lineup")))
  .withColumn(
    "team",
    when(array_contains(col("homeLineup"), col("playerId")), col("homeTeam"))
    .otherwise(col("awayTeam"))
  )
  .withColumn(
    "minutes",
    when(col("playerId") === col("homeSub1.playerOut"), col("homeSub1.minute"))
      .when(col("playerId") === col("homeSub2.playerOut"), col("homeSub2.minute"))
      .when(col("playerId") === col("homeSub3.playerOut"), col("homeSub3.minute"))
      .when(col("playerId") === col("awaySub1.playerOut"), col("awaySub1.minute"))
      .when(col("playerId") === col("awaySub2.playerOut"), col("awaySub2.minute"))
      .when(col("playerId") === col("awaySub3.playerOut"), col("awaySub3.minute"))
      .otherwise(col("matchLength"))
  )
  .cache()

val substitutePlayersDF = combinedMatchesInfoDF
  .select(
    col("matchId"),
    col("homeBench"),
    col("homeTeam"),
    col("awayTeam"),
    col("matchLength"),
    explode(array(col("homeSub1"), col("homeSub2"), col("homeSub3"), col("awaySub1"), col("awaySub2"), col("awaySub3"))).as("substitution")
  )
  .withColumn("playerId", col("substitution.playerIn"))
  .withColumn(
    "team",
    when(array_contains(col("homeBench"), col("playerId")), col("homeTeam"))
    .otherwise(col("awayTeam"))
  )
  .cache()

val startingPlayersTrimmedDF = startingPlayersDF
  .select("playerId", "matchId", "minutes")

val substitutePlayersTrimmedDF = substitutePlayersDF
  .select(
    col("substitution.playerIn").as("playerId"),
    col("matchId"),
    (col("matchLength") - col("substitution.minute")).as("minutes")
  )

val playerMinutesDF = startingPlayersTrimmedDF
  .union(substitutePlayersTrimmedDF)
  .groupBy(col("playerId"))
  .agg(sum(col("minutes")).as("minutes"))
  .withColumnRenamed("playerId", "id")

val playerDetailsDF = playerMinutesDF
  .join(playersInfoDF, col("id") === col("playerId"))
  .withColumn("player", concat(col("firstName"), lit(" "), col("lastName")))
  .select(col("role"), col("player"), col("birthArea"), col("minutes"))

val playtimeRankingByRole = Window.partitionBy("role").orderBy(col("minutes").desc)
val orderedPlayersDF = playerDetailsDF.withColumn("Pos", row_number().over(playtimeRankingByRole))

val mostMinutesDF: DataFrame = orderedPlayersDF
  .filter(col("Pos") === 1)
  .select(col("role"), col("player"), col("birthArea"), col("minutes"))
  .orderBy(col("minutes").desc)

println("The players with the most minutes played in season 2017-2018 for each player role:")
mostMinutesDF.show(false)

// COMMAND ----------

val goalEventsWithTimeDF = events2017_2018DF
  .filter((col("event").notEqual("Save attempt")) && 
    (array_contains(col("tags"), "Goal") || array_contains(col("tags"), "Own goal")))
  .withColumn(
    "eventTimeInMinutes",
    when(col("eventPeriod") === "2H", col("eventTime") / 60 + 45)
      .otherwise(col("eventTime") / 60)
  )
  .withColumn("isOwnGoal", array_contains(col("tags"), "Own goal"))
  .withColumn(
    "isFirstPeriodExtraTimeGoal",
    col("eventPeriod") === "1H" && col("eventTime") > 2700
  )
  .select("matchId", "eventTeam", "eventTimeInMinutes", "isFirstPeriodExtraTimeGoal", "isOwnGoal")

//Get the last first half extra time goal in case there are multiple
val lastExtraTimeGoalDF = goalEventsWithTimeDF
  .filter(col("isFirstPeriodExtraTimeGoal"))
  .groupBy("matchId")
  .agg(max("eventTimeInMinutes").as("lastExtraTimeGoal"))

val allPlayersFieldStatusDF = startingPlayersDF
  .withColumn(
    "onFieldStart",
    when(col("playerId") === col("homeSub1.playerIn"), col("homeSub1.minute"))
      .when(col("playerId") === col("homeSub2.playerIn"), col("homeSub2.minute"))
      .when(col("playerId") === col("homeSub3.playerIn"), col("homeSub3.minute"))
      .when(col("playerId") === col("awaySub1.playerIn"), col("awaySub1.minute"))
      .when(col("playerId") === col("awaySub2.playerIn"), col("awaySub2.minute"))
      .when(col("playerId") === col("awaySub3.playerIn"), col("awaySub3.minute"))
      .otherwise(0)
  )
  .withColumn(
    "onFieldEnd",
      when(col("playerId") === col("homeSub1.playerOut"), col("homeSub1.minute"))
        .when(col("playerId") === col("homeSub2.playerOut"), col("homeSub2.minute"))
        .when(col("playerId") === col("homeSub3.playerOut"), col("homeSub3.minute"))
        .when(col("playerId") === col("awaySub1.playerOut"), col("awaySub1.minute"))
        .when(col("playerId") === col("awaySub2.playerOut"), col("awaySub2.minute"))
        .when(col("playerId") === col("awaySub3.playerOut"), col("awaySub3.minute"))
        .otherwise(col("matchLength"))
  )
  .select("matchId", "team", "playerId", "onFieldStart", "onFieldEnd")
  .union(
    substitutePlayersDF.select(
      col("matchId"),
      col("team"),
      col("substitution.playerIn").as("playerId"),
      col("substitution.minute").as("onFieldStart"),
      col("matchLength").as("onFieldEnd")
    )
  )

val adjustedFieldStatusDF = allPlayersFieldStatusDF
  .join(lastExtraTimeGoalDF, "matchId", "left_outer")

val goalContributionsDF = goalEventsWithTimeDF
  .join(adjustedFieldStatusDF, "matchId")
  .filter(
    col("eventTimeInMinutes").between(col("onFieldStart"), col("onFieldEnd")) ||
      (col("isFirstPeriodExtraTimeGoal") &&
        col("onFieldEnd") >= 45 &&
        col("onFieldEnd") <= col("lastExtraTimeGoal"))
  )
  .withColumn(
    "contribution",
    when(col("isOwnGoal"),
      when(col("eventTeam") === col("team"), -1).otherwise(1)
      ).otherwise(
        when(col("eventTeam") === col("team"), 1).otherwise(-1)
      )
  )
  .groupBy("playerId")
  .agg(sum("contribution").as("plusMinus"))
  .select(col("playerId").as("id"), col("plusMinus"))

val contributorDetailsDF = goalContributionsDF
  .join(playersInfoDF, col("id") === col("playerId"))
  .withColumn("player", concat(col("firstName"), lit(" "), col("lastName")))
  .select(col("player"), col("birthArea"), col("role"), col("plusMinus"))

val topPlayers: DataFrame = contributorDetailsDF
  .filter(col("plusMinus") > 65)
  .orderBy(desc("plusMinus"), col("player"))

println("The players with higher than +65 for the plus-minus statistics in season 2017-2018:")
topPlayers.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced Task 3 - Image data and pixel colors (2 points)
// MAGIC
// MAGIC This advanced task involves loading in PNG image data and complementing JSON metadata into Spark data structure. And then determining the colors of the pixels in the images, and finding the answers to several color related questions.
// MAGIC
// MAGIC The folder `assignment/openmoji/color` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains collection of PNG images from [OpenMoji](https://openmoji.org/) project.
// MAGIC
// MAGIC The JSON Lines formatted file `assignment/openmoji/openmoji.jsonl` contains metadata about the image collection. Only a portion of the images are included as source data for this task, so the metadata file contains also information about images not considered in this task.
// MAGIC
// MAGIC #### Data description and helper functions
// MAGIC
// MAGIC The image data considered in this task can be loaded into a Spark data frame using the `image` format: [https://spark.apache.org/docs/3.5.0/ml-datasource.html](https://spark.apache.org/docs/3.5.0/ml-datasource.html). The resulting data frame contains a single column which includes information about the filename, image size as well as the binary data representing the image itself. The Spark documentation page contains more detailed information about the structure of the column.
// MAGIC
// MAGIC Instead of using the images as source data for machine learning tasks, the binary image data is accessed directly in this task.<br>
// MAGIC You are given two helper functions to help in dealing with the binary data:
// MAGIC
// MAGIC - Function `toPixels` takes in the binary image data and the number channels used to represent each pixel.
// MAGIC     - In the case of the images used in this task, the number of channels match the number bytes used for each pixel.
// MAGIC     - As output the function returns an array of strings where each string is hexadecimal representation of a single pixel in the image.
// MAGIC - Function `toColorName` takes in a single pixel represented as hexadecimal string.
// MAGIC     - As output the function returns a string with the name of the basic color that most closely represents the pixel.
// MAGIC     - The function uses somewhat naive algorithm to determine the name of the color, and does not always give correct results.
// MAGIC     - Many of the pixels in this task have a lot of transparent pixels. Any such pixel is marked as the color `None` by the function.
// MAGIC
// MAGIC With the help of the given functions it is possible to transform the binary image data to an array of color names without using additional libraries or knowing much about image processing.
// MAGIC
// MAGIC The metadata file given in JSON Lines format can be loaded into a Spark data frame using the `json` format: [https://spark.apache.org/docs/3.5.0/sql-data-sources-json.html](https://spark.apache.org/docs/3.5.0/sql-data-sources-json.html). The attributes used in the JSON data are not described here, but are left for you to explore. The original regular JSON formatted file can be found at [https://github.com/hfg-gmuend/openmoji/blob/master/data/openmoji.json](https://github.com/hfg-gmuend/openmoji/blob/master/data/openmoji.json).
// MAGIC
// MAGIC ### Tasks
// MAGIC
// MAGIC The target of the task is to combine the image data with the JSON data, determine the image pixel colors, and the find the answers to the following questions:
// MAGIC
// MAGIC - Which four images have the most colored non-transparent pixels?
// MAGIC - Which five images have the lowest ratio of colored vs. transparent pixels?
// MAGIC - What are the three most common colors in the Finnish flag image (annotation: `flag: Finland`)?
// MAGIC     - And how many percentages of the colored pixels does each color have?
// MAGIC - How many images have their most common three colors as, `Blue`-`Yellow`-`Black`, in that order?
// MAGIC - Which five images have the most red pixels among the image group `activities`?
// MAGIC     - And how many red pixels do each of these images have?
// MAGIC
// MAGIC It might be advisable to test your work-in-progress code with a limited number of images before using the full image set.<br>
// MAGIC You are free to choose your own approach to the task: user defined functions with data frames, RDDs/Datasets, or combination of both.

// COMMAND ----------

// separates binary image data to an array of hex strings that represent the pixels
// assumes 8-bit representation for each pixel (0x00 - 0xff)
// with `channels` attribute representing how many bytes are used for each pixel
def toPixels(data: Array[Byte], channels: Int): Array[String] = {
    data
        .grouped(channels)
        .map(dataBytes =>
            dataBytes
                .map(byte => f"${byte & 0xff}%02X")
                .mkString("")
        )
        .toArray
}

// COMMAND ----------

// naive implementation of picking the name of the pixel color based on the input hex representation of the pixel
// only works for OpenCV type CV_8U (mode=24) compatible input
def toColorName(hexString: String): String = {
    // mapping of RGB values to basic color names
    val colors: Map[(Int, Int, Int), String] = Map(
        (0, 0, 0)     -> "Black",  (0, 0, 128)     -> "Blue",   (0, 0, 255)     -> "Blue",
        (0, 128, 0)   -> "Green",  (0, 128, 128)   -> "Green",  (0, 128, 255)   -> "Blue",
        (0, 255, 0)   -> "Green",  (0, 255, 128)   -> "Green",  (0, 255, 255)   -> "Blue",
        (128, 0, 0)   -> "Red",    (128, 0, 128)   -> "Purple", (128, 0, 255)   -> "Purple",
        (128, 128, 0) -> "Green",  (128, 128, 128) -> "Gray",   (128, 128, 255) -> "Purple",
        (128, 255, 0) -> "Green",  (128, 255, 128) -> "Green",  (128, 255, 255) -> "Blue",
        (255, 0, 0)   -> "Red",    (255, 0, 128)   -> "Pink",   (255, 0, 255)   -> "Purple",
        (255, 128, 0) -> "Orange", (255, 128, 128) -> "Orange", (255, 128, 255) -> "Pink",
        (255, 255, 0) -> "Yellow", (255, 255, 128) -> "Yellow", (255, 255, 255) -> "White"
    )

    // helper function to round values of 0-255 to the nearest of 0, 128, or 255
    def roundColorValue(value: Int): Int = {
        if (value < 85) 0
        else if (value < 170) 128
        else 255
    }

    hexString.matches("[0-9a-fA-F]{8}") match {
        case true => {
            // for OpenCV type CV_8U (mode=24) the expected order of bytes is BGRA
            val blue: Int = roundColorValue(Integer.parseInt(hexString.substring(0, 2), 16))
            val green: Int = roundColorValue(Integer.parseInt(hexString.substring(2, 4), 16))
            val red: Int = roundColorValue(Integer.parseInt(hexString.substring(4, 6), 16))
            val alpha: Int = Integer.parseInt(hexString.substring(6, 8), 16)

            if (alpha < 128) "None"  // any pixel with less than 50% opacity is considered as color "None"
            else colors((red, green, blue))
        }
        case false => "None"  // any input that is not in valid format is considered as color "None"
    }
}

// COMMAND ----------

val imageDF = spark.read
  .format("image")
  .option("dropInvalid", true)
  .load("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/openmoji/color")

val metaDataDF = spark.read
  .json("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/openmoji/metadata/openmoji.jsonl")

val toPixelsUDF = udf((data: Array[Byte], channels: Int) => toPixels(data, channels))
val toColorNameUDF = udf((hexString: String) => toColorName(hexString))

val processedImageDF = imageDF
  .withColumn("pixels", toPixelsUDF(col("image.data"), col("image.nChannels")))
  .withColumn("colors", transform(col("pixels"), pixel => toColorNameUDF(pixel)))
  .withColumn("coloredPixels", filter(col("colors"), color => color =!= "None"))
  .withColumn("coloredPixelCount", size(col("coloredPixels")))
  .withColumn("totalPixelCount", size(col("pixels")))
  .withColumn("hexcodeFromAddress", regexp_extract(col("image.origin"), """.*\/([a-fA-F0-9-]+)\.png$""", 1))
  .withColumn("coloredToTransparentRatio", 
    col("coloredPixelCount") / greatest(col("totalPixelCount"), lit(1))
  )

val annotatedImagesDF = processedImageDF
  .join(metaDataDF, col("hexcodeFromAddress") === col("hexcode"))
  .select("image", "coloredPixels", "coloredPixelCount", "coloredToTransparentRatio", "annotation", "group")
  .cache()

// The annotations for the four images with the most colored non-transparent pixels
val mostColoredPixels: Array[String] = annotatedImagesDF
  .select("annotation", "coloredPixelCount")
  .orderBy(desc("coloredPixelCount"))
  .limit(4)
  .select("annotation")
  .as[String]
  .collect()

println("The annotations for the four images with the most colored non-transparent pixels:")
mostColoredPixels.foreach(image => println(s"- ${image}"))
println("============================================================")

// The annotations for the five images having the lowest ratio of colored vs. transparent pixels
val leastColoredPixels: Array[String] = annotatedImagesDF
  .select("annotation", "coloredToTransparentRatio")
  .orderBy(asc("coloredToTransparentRatio"))
  .limit(5)
  .select("annotation")
  .as[String]
  .collect()

println("The annotations for the five images having the lowest ratio of colored vs. transparent pixels:")
leastColoredPixels.foreach(image => println(s"- ${image}"))
println("============================================================")

// COMMAND ----------

val finnishFlagDF = annotatedImagesDF
  .filter(col("annotation") === "flag: Finland")
  .select("coloredPixels", "coloredPixelCount")

val finnishFlagColorCountsDF = finnishFlagDF
  .select(explode(col("coloredPixels")).as("color"))
  .groupBy("color")
  .count()

val totalColoredPixels = finnishFlagDF
  .select(col("coloredPixelCount"))
  .first()
  .getInt(0)
  .toLong

val colorPercentagesDF = finnishFlagColorCountsDF
  .withColumn("percentage", round(col("count") / totalColoredPixels * 100, 2))

val topColorsDF = colorPercentagesDF
  .orderBy(desc("count"))
  .limit(3)

// The three most common colors in the Finnish flag image:
val finnishFlagColors: Array[String] = topColorsDF.select("color").as[String].collect()

// The percentages of the colored pixels for each common color in the Finnish flag image:
val finnishColorShares: Array[Double] = topColorsDF.select("percentage").as[Double].collect()

println("The colors and their percentage shares in the image for the Finnish flag:")
finnishFlagColors.zip(finnishColorShares).foreach({case (color, share) => println(s"- color: ${color}, share: ${share}")})
println("============================================================")

val colorCountsDF = annotatedImagesDF
  .select(col("annotation"), explode(col("coloredPixels")).as("color"))
  .groupBy("annotation", "color")
  .count()

val colorCountRanking = Window.partitionBy("annotation").orderBy(desc("count"))
val rankedColorsDF = colorCountsDF.withColumn("rank", rank().over(colorCountRanking))

val filteredImagesDF = rankedColorsDF
  .filter(col("rank") <= 3)
  .groupBy("annotation")
  .agg(collect_list(struct(col("rank"), col("color"))).as("rankedColors"))
  .filter(
    col("rankedColors").getItem(0).getField("color") === "Blue" &&
    col("rankedColors").getItem(1).getField("color") === "Yellow" &&
    col("rankedColors").getItem(2).getField("color") === "Black"
  )

// The number of images that have their most common three colors as, Blue-Yellow-Black, in that exact order:
val blueYellowBlackCount: Long = filteredImagesDF.count()

println(s"The number of images that have, Blue-Yellow-Black, as the most common colors: ${blueYellowBlackCount}")
println("============================================================")

// COMMAND ----------

val activitiesImagesWithRedPixelsDF = annotatedImagesDF
  .filter(col("group") === "activities")
  .withColumn(
    "redPixelCount",
    size(filter(col("coloredPixels"), color => color === "Red"))
  )
  .select("annotation", "redPixelCount")

val topRedImagesDF = activitiesImagesWithRedPixelsDF
  .orderBy(desc("redPixelCount"))
  .limit(5)

// The annotations for the five images with the most red pixels among the image group activities:
val redImageNames: Array[String] = topRedImagesDF.select("annotation").as[String].collect()

// The number of red pixels in the five images with the most red pixels among the image group activities:
val redPixelAmounts: Array[Long] = topRedImagesDF.select("redPixelCount").as[Long].collect()

println("The annotations and red pixel counts for the five images with the most red pixels among the image group 'activities':")
redImageNames.zip(redPixelAmounts).foreach({case (color, count) => println(s"- ${color} (red pixels: ${count})")})
println("============================================================")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced Task 4 - Machine learning tasks (2 points)
// MAGIC
// MAGIC This advanced task involves experimenting with the classifiers provided by the Spark machine learning library. Time series data collected in the [ProCem](https://www.senecc.fi/projects/procem-2) research project is used as the training and test data. Similar data in a slightly different format was used in the first tasks of weekly exercise 3.
// MAGIC
// MAGIC The folder `assignment/energy/procem_13m.parquet` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains the time series data in Parquet format.
// MAGIC
// MAGIC #### Data description
// MAGIC
// MAGIC The dataset contains time series data from a period of 13 months (from the beginning of May 2023 to the end of May 2024). Each row contains the average of the measured values for a single minute. The following columns are included in the data:
// MAGIC
// MAGIC | column name        | column type   | description |
// MAGIC | ------------------ | ------------- | ----------- |
// MAGIC | time               | long          | The UNIX timestamp in second precision |
// MAGIC | temperature        | double        | The temperature measured by the weather station on top of Sähkötalo (`°C`) |
// MAGIC | humidity           | double        | The humidity measured by the weather station on top of Sähkötalo (`%`) |
// MAGIC | wind_speed         | double        | The wind speed measured by the weather station on top of Sähkötalo (`m/s`) |
// MAGIC | power_tenants      | double        | The total combined electricity power used by the tenants on Kampusareena (`W`) |
// MAGIC | power_maintenance  | double        | The total combined electricity power used by the building maintenance systems on Kampusareena (`W`) |
// MAGIC | power_solar_panels | double        | The total electricity power produced by the solar panels on Kampusareena (`W`) |
// MAGIC | electricity_price  | double        | The market price for electricity in Finland (`€/MWh`) |
// MAGIC
// MAGIC There are some missing values that need to be removed before using the data for training or testing. However, only the minimal amount of rows should be removed for each test case.
// MAGIC
// MAGIC ### Tasks
// MAGIC
// MAGIC - The main task is to train and test a machine learning model with [Random forest classifier](https://spark.apache.org/docs/3.5.0/ml-classification-regression.html#random-forests) in six different cases:
// MAGIC     - Predict the month (1-12) using the three weather measurements (temperature, humidity, and wind speed) as input
// MAGIC     - Predict the month (1-12) using the three power measurements (tenants, maintenance, and solar panels) as input
// MAGIC     - Predict the month (1-12) using all seven measurements (weather values, power values, and price) as input
// MAGIC     - Predict the hour of the day (0-23) using the three weather measurements (temperature, humidity, and wind speed) as input
// MAGIC     - Predict the hour of the day (0-23) using the three power measurements (tenants, maintenance, and solar panels) as input
// MAGIC     - Predict the hour of the day (0-23) using all seven measurements (weather values, power values, and price) as input
// MAGIC - For each of the six case you are asked to:
// MAGIC     1. Clean the source dataset from rows with missing values.
// MAGIC     2. Split the dataset into training and test parts.
// MAGIC     3. Train the ML model using a Random forest classifier with case-specific input and prediction.
// MAGIC     4. Evaluate the accuracy of the model with Spark built-in multiclass classification evaluator.
// MAGIC     5. Further evaluate the accuracy of the model with a custom build evaluator which should do the following:
// MAGIC         - calculate the percentage of correct predictions
// MAGIC             - this should correspond to the accuracy value from the built-in accuracy evaluator
// MAGIC         - calculate the percentage of predictions that were at most one away from the correct predictions taking into account the cyclic nature of the month and hour values:
// MAGIC             - if the correct month value was `5`, then acceptable predictions would be `4`, `5`, or `6`
// MAGIC             - if the correct month value was `1`, then acceptable predictions would be `12`, `1`, or `2`
// MAGIC             - if the correct month value was `12`, then acceptable predictions would be `11`, `12`, or `1`
// MAGIC         - calculate the percentage of predictions that were at most two away from the correct predictions taking into account the cyclic nature of the month and hour values:
// MAGIC             - if the correct month value was `5`, then acceptable predictions would be from `3` to `7`
// MAGIC             - if the correct month value was `1`, then acceptable predictions would be from `11` to `12` and from `1` to `3`
// MAGIC             - if the correct month value was `12`, then acceptable predictions would be from `10` to `12` and from `1` to `2`
// MAGIC         - calculate the average probability the model predicts for the correct value
// MAGIC             - the probabilities for a single prediction can be found from the `probability` column after the predictions have been made with the model
// MAGIC - As the final part of this advanced task, you are asked to do the same experiments (training+evaluation) with two further cases of your own choosing:
// MAGIC     - you can decide on the input columns yourself
// MAGIC     - you can decide the predicted attribute yourself
// MAGIC     - you can try some other classifier other than the random forest one if you want
// MAGIC
// MAGIC In all cases you are free to choose the training parameters as you wish.<br>
// MAGIC Note that it is advisable that while you are building your task code to only use a portion of the full 13-month dataset in the initial experiments.

// COMMAND ----------

// the structure of the code and the output format is left to the group's discretion
// the example output notebook can be used as inspiration
