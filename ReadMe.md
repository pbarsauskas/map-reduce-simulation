##Framework to simulate MapReduce process
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

