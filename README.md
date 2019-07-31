# Respostas:

Qual o objetivo do comando cache em Spark?
Persistir os dados em memória, ao invés do disco, para um processamento mais rápido dos datasets.


O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?
O Spark é um framework criado para esse próposito, que pode armazenar e processar os dados em memória e em disco, ao contrário do MapReduce que só utiliza disco.


Qual é a função do SparkContext?
Estabelecer conexão com os ambientes de execução do Spark.


Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
É uma coleção de dados imutáveis e distribuída que podem ser paralelamente processadas em várias partições de um cluster, 
além de tolerante a falha pois permite o reprocessamento somente partição com erro.
 

GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
Pois o reduceByKey combina os dados antes de enviar para o shuffle, ao contrário do groupByKey, reduzindo a quantidade de dados transmitidos e processados pelas aplicações.


Explique o que o código Scala abaixo faz.

val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
		.map(word => (word, 1))
		.reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")

Lê um arquivo texto de um HDFS, separa e enumera cada palavra, salva o resultado de cada palavra e sua contagem no HDFS.



1. Número de hosts únicos.
137979

2. O total de erros 404.
Total 404 errors: 20901

3. Os 5 URLs que mais causaram erro 404.
hoohoo.ncsa.uiuc.edu 251
piweba3y.prodigy.com 157
jbiagioni.npt.nuwc.navy.mil 132
piweba1y.prodigy.com 114
www-d4.proxy.aol.com 91

4. Quantidade de erros 404 por dia.
01/Jul/1995 316
02/Jul/1995 291
03/Jul/1995 474
04/Jul/1995 359
05/Jul/1995 497
06/Jul/1995 640
07/Jul/1995 570
08/Jul/1995 302
09/Jul/1995 348
10/Jul/1995 398
11/Jul/1995 471
12/Jul/1995 471
13/Jul/1995 532
14/Jul/1995 413
15/Jul/1995 254
16/Jul/1995 257
17/Jul/1995 406
18/Jul/1995 465
19/Jul/1995 639
20/Jul/1995 428
21/Jul/1995 334
22/Jul/1995 192
23/Jul/1995 233
24/Jul/1995 328
25/Jul/1995 461
26/Jul/1995 336
27/Jul/1995 336
28/Jul/1995 94
01/Aug/1995 243
03/Aug/1995 304
04/Aug/1995 346
05/Aug/1995 236
06/Aug/1995 373
07/Aug/1995 537
08/Aug/1995 391
09/Aug/1995 279
10/Aug/1995 315
11/Aug/1995 263
12/Aug/1995 196
13/Aug/1995 216
14/Aug/1995 287
15/Aug/1995 327
16/Aug/1995 259
17/Aug/1995 271
18/Aug/1995 256
19/Aug/1995 209
20/Aug/1995 312
21/Aug/1995 305
22/Aug/1995 288
23/Aug/1995 345
24/Aug/1995 420
25/Aug/1995 415
26/Aug/1995 366
27/Aug/1995 370
28/Aug/1995 410
29/Aug/1995 420
30/Aug/1995 571
31/Aug/1995 526


5. O total de bytes retornados.
Total bytes: 65524314915
