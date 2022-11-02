# Web Scraping Automation using Airflow

This repository is part of the following tutorial:

https://medium.com/@fadlil.ismail/web-scraping-automation-using-apache-airflow-d8c02f3b1a12

## Requirements
To run this Airflow DAG, you need to install the following programs on Windows Subsystem for Linux (I used Ubuntu 22.04):
1. python3
2. python3-pip
3. Postgresql

After installing all the programs above, you need to install Apache Airflow on your WSL and activate it.
You can follow this tutorial to install and setting up the Airflow : 

https://www.youtube.com/watch?v=Va_NMDoDqLQ&t=680s

But don't forget to set AIRFLOW_HOME using this command :

```sh
export AIRFLOW_HOME=~/airflow 
```

before you run :

```sh
airflow db init
```

You can find the airflow directory on this path :


```sh
\\wsl$\Ubuntu-22.04\home\{your wsl username}\airflow
```


or by running this command on your WSL : 

```sh
'explorer.exe .' (quote signs not included)
```

You can put the DAG file in the 'dags' folder, if the 'dags' folder doesn't exist you can create it first.

