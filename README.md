# Group 8 Cloud and BigData


| [Website](https://josevelascosantos.github.io/Group_8_Cloud_and_BigData/website/) |
| ----------- |

## How does static data processing work?
![Static file diagram](./website/images/Diagrama%20estático.png)


## How does dynamic data processing work?
![Dynamic file diagram](./website/images/Diagrama%20dinámico.png)

## Commands
### Dynamic
In order to use the application you should execute the following commands:  

1. python3 ./getData.py <API-KEY> <PERIOD> <RESPONSE_LIMIT>  
2. spark-submit days_setRes.py  
3. spark-submit getAVGs.py
  
### Static

In order to use the application you should execute the following commands:  

1. spark-submit filtre.py
2. python pandasfilter.py
3. spark-submit process.py

## Performance
![Static files](./website/images/Static%20files.svg)
![Dynamic files](./website/images/Dynamic%20files.svg)

(VM details: 4 vCPU 3.6GB memory)
(PC details: 4 CPU 16GB memory)

## Conclusions
In static files you can see an advantage of processing in a VM with Spark compared to a PC with Spark.
As the number of files increases, the greater the difference in time required to achieve processing.

With dynamic data, you can't directly run on an average pc. Even so, it can be seen that the increase in files is not directly proportional to the increase in time when analyzing one file against many.
