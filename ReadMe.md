
## Framework to simulate MapReduce process

This is an attempt to create a simplified MapReduce workflow simulation.
Data (in csv files) is processed in parallel (to simulate processes in multiple nodes). 
Map operation outputs are aggregated by the reduce task and the final results is saved to disk in csv format.
The framework has a basic command line interface which allows scheduling of jobs. 

#### Prerequisites

Python 3.6
https://www.python.org/downloads/

Pandas 0.22
https://pandas.pydata.org

#### Instructions
To launch framework command line interface, please, run:
```
python3.6 map-reduce-simulation.py
```
To run a job, please, execute:
```
run JobName
```
Job results are saved in ../data directory.

You can also schedule jobs by using 'schedule' command (pass time in seconds as second argument). E.g.:
```
schedule JobName 5
```
To exit command line interface, please, run:
```
quit
```
#### License

This project is licensed under the MIT License - see the [License.md](https://github.com/pbarsauskas/map-reduce-simulation/blob/master/License.md) file for details.

