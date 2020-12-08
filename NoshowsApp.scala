import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrameNaFunctions, Row}
import org.apache.spark.ml.feature.{StringIndexer,VectorAssembler,IndexToString,VectorIndexer,Bucketizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator,BinaryClassificationEvaluator}
									   
import org.apache.spark.sql.functions.{sum,when,row_number,max,datediff,countDistinct}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.classification.{RandomForestClassificationModel,RandomForestClassifier}
										   
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.PipelineModel

import java.io.File
import java.io.PrintWriter
import scala.io.Source

object NoshowsApp
 {
  def main(args: Array[String])
  {
        // File di output
        val resultsFile = "/home/ubuntu/dataset/noshows/results.txt"

        // Avvio del counter temporale
        val t0 = System.currentTimeMillis()

		// Creazione della sessione
        val spark = SparkSession.builder.appName("NoshowsApp").getOrCreate()

        import spark.implicits._

		// Preparazione dei campi dello schema
        val patientId = StructField("PatientId", DataTypes.LongType)
        val appointmentID = StructField("AppointmentID", DataTypes.LongType)
        val gender = StructField("Gender", DataTypes.StringType)
        val scheduledDay =  StructField("ScheduledDay", DataTypes.TimestampType)
        val appointmentDay =  StructField("AppointmentDay", DataTypes.TimestampType)
        val age = StructField("Age", DataTypes.IntegerType)
        val neighbourhood = StructField("Neighbourhood", DataTypes.StringType)
        val scholarship = StructField("Scholarship", DataTypes.IntegerType)
        val hipertension = StructField("Hipertension", DataTypes.IntegerType)
        val diabetes = StructField("Diabetes", DataTypes.IntegerType)
        val alcoholism = StructField("Alcoholism", DataTypes.IntegerType)
        val handcap = StructField("Handcap", DataTypes.IntegerType)
        val sms_received = StructField("SMS_received", DataTypes.IntegerType)
        val noshow = StructField("No_show", DataTypes.StringType)

		// I campi vanno inseriti in un Array
        val fields = Array(
		patientId,
		appointmentID,
		gender, 
		scheduledDay,
		appointmentDay,
		age, 
		neighbourhood,
		scholarship,
		hipertension,
		diabetes,
		alcoholism,
		handcap,
		sms_received,
		noshow
		)
		
		// Creazione di una struttura per lo schema
        val schema = StructType(fields)

		// Lettura del dataset da un file CSV su filesystem hdfs
        val inDataDF = spark.read
		.option("header", true)
		.schema(schema)
		.csv("hdfs://namenode:9000/dataset/noshows/noshowsDS.csv")
		.cache()

		// Preprocessing: prima trasformazione del DF
        val preProcessDateDF = inDataDF.select(
		$"PatientId",
		$"AppointmentID",
		$"Gender", 
		datediff($"AppointmentDay",$"ScheduledDay").as("Notice"), 
		$"Age",
		$"Neighbourhood",
		$"Scholarship",
		$"Hipertension",
		$"Diabetes",
		$"Alcoholism",
		$"Handcap",
		$"SMS_received",
		$"No_show"
		)

		// Definizione di una serie di intervalli discreti per il bucketizer
        val splits = Array(
		Double.NegativeInfinity,
		0.0,25.0,50.0,75.0,100.0,
		Double.PositiveInfinity
		)

		// Creazione del bucketizer con gli intervalli definiti in precedenza
        val bucketizer = new Bucketizer()
		.setInputCol("Age")
		.setOutputCol("age_category")
		.setSplits(splits)

		// Preprocessing: trasformazione finale del DF
        val preProcessAgeBinsDF = bucketizer.transform(preProcessDateDF)

		// Feature engineering: categorizzazione di Neighbourhood
        val neighbourhoodIndexer = new StringIndexer()
		.setInputCol("Neighbourhood")
		.setOutputCol("NeighbourhoodCat")
		.fit(preProcessDateDF)
		
        val nDF = neighbourhoodIndexer.transform(preProcessAgeBinsDF)
		
		// Feature engineering: categorizzazione di Gender
        val genderIndexer = new StringIndexer()
		.setInputCol("Gender")
		.setOutputCol("GenderCat")
		.fit(nDF)
		
        val df = genderIndexer.transform(nDF)
		
		// categorizzazione di della label No_show
        val labelIndexer = new StringIndexer()
		.setInputCol("No_show")
		.setOutputCol("indexedLabel")

		// Ricampionamento del dataset per riequilibrare le classi
        val sampleDF = df.stat.sampleBy("No_show", Map("Yes" -> 1.0, "No" -> .5), 0)

		// Feature engineering: assemblaggio delle feature in unico vettore
        val assembler = new VectorAssembler()
		.setInputCols( Array(
								"GenderCat",
								"Notice", 
								"age_category",
								"NeighbourhoodCat",
								"Scholarship",
								"Hipertension",
								"Diabetes", 
								"Alcoholism", 
								"Handcap",
								"SMS_received"
								)
						)
								.setOutputCol("features")

		// Applicazione della trasformazione del VectorAssembler
        val df2 = assembler.transform(sampleDF)
		
		val maxCat = 81

		// Le feature assemblate vengono indicizzate mediante VectorIndexer
		// Viene anche specificato il massimo numero di categorie
        val featureIndexer = new VectorIndexer()
		.setInputCol("features")
		.setOutputCol("indexedFeatures")
		.setMaxCategories(maxCat)
		.fit(df2)
		
		// Impostiamo maxBins al numero massimo di categorie 
        val maxBins = maxCat 
		// Il numero di alberi utilizzato dal RandomForest
        val numTrees = 20

		// Usiamo il RandomForest come algoritmo di learning
        val rf = new RandomForestClassifier()
		.setLabelCol("indexedLabel")
		.setFeaturesCol("indexedFeatures")
		.setNumTrees(numTrees)
		.setMaxBins(maxBins)

		// Inizializziamo la pipeline specificando la label indicizzata,
		// le feature indicizzate e l'algoritmo di learning (RandomForest)
        val pipeline = new Pipeline().setStages( Array(labelIndexer, featureIndexer, rf))

		// Effettuiamo uno split del dataset: 80% training set, 20% test set
        val Array(trainingData, testData) = df2.randomSplit(Array(0.8, 0.2), 11L)

		// Apprendimento e costruzione del modello
        val model = pipeline.fit(trainingData)
		// Applicazione del modello al test set
        val predictions = model.transform(testData)       
		
		// Costruzione di un valutatore delle prestazioni del modello
        val evaluator = new MulticlassClassificationEvaluator()
		.setLabelCol("indexedLabel")
		.setPredictionCol("prediction")
		.setMetricName("accuracy")
		
		// Valutazione della metrica richiesta (Accuratezza)
        val accuracy = evaluator.evaluate(predictions)

		//======== Cross Validation ==============

		// Creazione di una griglia di parametri
        val paramGrid = new ParamGridBuilder()
		.addGrid(rf.maxBins, Array(81,85))
		.addGrid(rf.maxDepth, Array(10, 12))
		.addGrid(rf.impurity, Array("entropy", "gini"))
		.build()
		
		// Utilizzeremo un valutatore binario
        val binEvaluator = new BinaryClassificationEvaluator().setLabelCol("indexedLabel")
		// Useremo una 10-fold Cross validation: il dataset sarà diviso in 10 partizioni
        val kFolds = 10
		
		// Creazione dell' oggetto CrossValidator 
        val cv = new CrossValidator()
		.setEstimator(pipeline)
		.setEvaluator(binEvaluator)
		.setEstimatorParamMaps(paramGrid)
		.setNumFolds(kFolds)
		
		// Avvio della Cross Validation
        val crossValidatorModel = cv.fit(df2)
		
		// Applicazione del modello ottenuto con la CV al test set
        val finalPredictions = crossValidatorModel.transform(testData)

		//-------- Fine CV ---------
		
		// Calcolo del tempo impiegato        
        val t1 = System.currentTimeMillis()
        val totalTime= (t1 - t0)/ 1000
		val minutes = totalTime / 60
		val seconds = totalTime % 60

		// Scrittura del report finale sul file dei risultati
        val writer = new PrintWriter(new File(resultsFile))
        writer.write("Results report: \n")
        writer.write("------------ \n")
        writer.write("Accuracy (%) = " + (accuracy*100) +"%  [Before CV] \n")

        val finalAccuracy = binEvaluator.evaluate(finalPredictions)
        writer.write("Accuracy (%) = " + (finalAccuracy*100) +"%  [After CV] \n")
        writer.write("Task completed in " + minutes + " minutes, " + seconds + " seconds \n")
        writer.write("Best model params: \n");

		// Scriviamo i dati del miglior modello ottenuto dalla CV
        crossValidatorModel
		.bestModel
		.asInstanceOf[PipelineModel]
		.stages
		.foreach(stage => writer.write( stage.extractParamMap + "\n") )

        writer.close()
		
		// Chiusura della sessione
        spark.stop()
  }
   }