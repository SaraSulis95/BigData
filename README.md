# BigData
Progetto BigData2020 di Fabrizio Chelo e Sara Sulis

COMPILAZIONE
------------

Per compilare correttamente il file occorre avere una struttura di directory del codice sorgente organizzata come mostrato:
App
├── build.sbt
└── src
    └──main
        └──scala
            └──NoshowsApp.scala

La compilazione avviene dall’interno delle directory App.
App può essere nominata anche diversamente, ma è importante che al suo interno abbia la struttura mostrata e il file sorgente risieda nel path indicato.
Anche il file sbt deve trovarsi nella root della struttura, come mostrato.
Il file build.sbt deve contenere tutti i riferimenti ai package utilizzati nell’applicazione, e rappresenta in Scala l’analogo del makefile per il C.
Segue il suo listato:
name := "noshows Project"
version := "1.0"
scalaVersion := "2.12.10"
libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "3.0.1",
        "org.apache.spark" %% "spark-sql" % "3.0.1",
        "org.apache.spark" %% "spark-mllib" % "3.0.1")

Prima di poter compilare l’applicazione, occorre installare il tool sbt (Simple Build Tool, il suo acronimo originale), poiché nella distribuzione normale di Ubuntu non è presente.
Non è necessario installare altri tool Scala, in quanto il packet manager provvederà ad installare automaticamente le dipendenze richieste da sbt. Invece è necessario che sia presente il JDK (nel nostro caso abbiamo installato OpenJDK 1.8.0).
Su Ubuntu, sono necessari i seguenti comandi:
$ echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
$ curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
$ sudo apt-get update
$ sudo apt-get install sbt

Terminata l’installazione il comando sbt è disponibile dalla linea di comando della shell, dunque possiamo compilare la nostra applicazione, avendo cura di lanciare il comando nella root del progetto, quella dove risiede il file build.sbt, come mostrato nella struttura ad albero vista in precedenza.
La compilazione avviene con il seguente comando nella shell:
$ sbt package

Questo comando impiega del tempo, la prima volta che viene eseguito, perché effettua tutti gli aggiornamenti necessari ai package coinvolti nel processo di compilazione.
Dalla seconda in poi, la compilazione è incrementale e i tempi sono molto più ridotti.

La struttura vista in precedenza viene aggiornata con numerose directory, la più importante delle quali, ai fini dell’esecuzione dell’applicazione è target, dove risiede il jar da mandare in esecuzione.
App
├── build.sbt
├── project
│   ├── build.properties
│   └── target
│       ├── config-classes
│       ├── scala-2.12
│       └── streams
├── src
│   └── main
│      └── scala
│	        └── NoshowsApp.scala
└── target
    ├── global-logging
    ├── scala-2.12
                ├── classes
                └── noshows-project_2.12-1.0.jar
                
              
ESECUZIONE
-----------
Per eseguire l'applicazione occorre lanciare il seguente comando

<spark dir>/bin/spark-submit \
  --class "NoshowsApp" \
  --master spark://ip-172-31-24-235.us-east-2.compute.internal:7077 \
  --driver-memory 3g \
  --executor-memory 1500m \
 target/scala-2.12/noshows-project_2.12-1.0.jar
 
