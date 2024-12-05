# insurance-datamart-processing

>## This code running on Airflow DAG
>Please follow this path place to run the code!!!

```
dags
├── queries
│   ├── etl_assurance_process.py
├── internal_data
│   ├── DE_assumptions.csv
│   ├── DE_claims.csv
│   ├── DE_insurance_contracts.csv
├── dag_etl_assurance_process.py
```

>#### Task I do
>> Insert data to table using Airflow (i'm named it as landing_[csvName])
>>
>> Task Delete data from **datamart**
>>
>> Task Insert data to **datamart**


>##### Condition
> The csv data is not clear to me, because table claims and table insurance_contracts i assume it is level transaction. Its too hard to combine the data if i dont know the key join so the query to get FCF and CMS is not correct for me. Sorry if i can't provide correct information for this task.
