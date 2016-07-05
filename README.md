## Analysis of ArXiv.org database of scientific papers

<p align="center">
	<img src="https://github.com/jaimeps/distributed-computing-arxiv/blob/master/images/logos.png" width="250">
</p>

MSAN 694 - Distributed Computing <br />
Team: [D. Wen](https://github.com/davidjeffwen), [A. Romriell](https://github.com/asromriell), [J. Pastor](https://github.com/jaimeps), [J. Pollard](https://github.com/pollardJ)

### Data Description:  
Source: [ArXiv Electronic Archive of Scientific Papers](http://arxiv.org/) <br />
We analyzed the entire database of arXiv.org (1.6GB): <br />
- 1.26 million papers <br />
- 600,000 authors <br />
- 86,262,827 words <br />

### Goal:
Exploratory Data Analysis and Community Detection, implemented with three different technologies (postgreSQL, Hive and Spark) for performance comparison.

### Experimental environment:
Local: <br /> 
>MacBook Pro 2.7 GHz Intel Core i5 16 GB 1867 MHz DDR3

Distributed: <br /> 
>4-node cluster of r3.xlarge (160GB) emr-4.6.0 <br /> 
>Hadoop distribution: Amazon 2.7.2 <br /> 
>Applications: Hive 1.0.0, Pig 0.14.0, Spark 1.6.1 <br /> 

### Summary results:
The following table summarizes the running time (in seconds) of the tasks in each of the different platforms (postgreSQL, Hive and SparkSQL):
<img src="https://github.com/jaimeps/distributed-computing-arxiv/blob/master/images/running_time.png">
As the queries grew in complexity (4 and 5), Hive and SparkSQL perform drastically better than PostgreSQL. In particular, we observed a reduction in running times between 80% and 97% using SparkSQL.

We concluded that - in the context of this problem - SparkSQL was the optimal tool given its speed, ease of use and flexibility.
<img src="https://github.com/jaimeps/distributed-computing-arxiv/blob/master/images/evaluation.png">


