# Group 8 Cloud and BigData

## How does static data processing work?
![Static file diagram](./website/images/Diagrama%20estático.png)


## How does dynamic data processing work?
![Dynamic file diagram](./website/images/Diagrama%20dinámico.png)

## Performance
![Static files](./website/images/Static%20files.svg)
![Dynamic files](./website/images/Dynamic%20files.svg)

## Conclusions
In static files you can see an advantage of processing in a VM with Spark compared to a PC with Spark.
As the number of files increases, the greater the difference in time required to achieve processing.

With dynamic data, you can't directly run on an average pc. Even so, it can be seen that the increase in files is not directly proportional to the increase in time when analyzing one file against many.