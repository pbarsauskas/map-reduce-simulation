from multiprocessing import Pool, Manager
from threading import Thread
from cmd import Cmd
import pandas as pd
import os, sys, sched, time, datetime, logging, json

class MapReduce:
    def __init__(self, queue):
        #In this MapReduce simualtion output from map opeartions is put to queue instead of writing to disk
        self.queue = queue
    def mapper(self, files, key, pars, queue):
        for i in pars['map']['logic']:
            exec(i) #Executes developer code.

    def reducer(self, files, path, pars, queue):
        df = pd.DataFrame()
        for i in pars['reduce']['logic']:
            exec(i) #Executes developer code.
        return df

class Main(Cmd):

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    def do_run(self, job):
        logging.info(self.get_time()+' '+ job+' execution started:')
        path = os.getcwd()

        #Load job parameter file
        try:
            pars = self.get_parameters(path, job)
        except Exception as e:
            logging.error(self.get_time()+' '+job+ ' parameter loading failed: ' + str(e))
            return
        else:
            logging.info(self.get_time() + ' ' + job + '     parameter loading completed')

        #Load datasets
        try:
           files = self.get_files(path, pars)
        except Exception as e:
            logging.error(self.get_time()+' '+job+ ' dataset loading failed: ' + str(e))
            return
        else:
            logging.info(self.get_time() + ' ' + job + '     dataset loading completed')

        # Map part
        try:
            manager = Manager()
            queue = manager.Queue()
            Job = MapReduce(queue)
            pool = Pool() #To parallelize dataset processing
            for i in range(0, len(files)):
                if isinstance(pars['dataset'], str):
                    pool.apply(Job.mapper, args=(files[i:i + 1], pars['key'], pars, queue))
                else:
                    pool.apply(Job.mapper, args=(files[i:i + 1], pars['key'][i], pars, queue))
            pool.close()
            pool.join()
        except Exception as e:
            logging.error(self.get_time()+' '+job+ ' map operation failed: ' + str(e))
            return
        else:
            logging.info(self.get_time() + ' ' + job + '     map operation completed')

        # Reduce part
        try:
            Job.reducer(files, path, pars, queue)
        except Exception as e:
            logging.error(self.get_time()+' '+job+ ' reduce operation failed: ' + str(e))
            return
        else:
            logging.info(self.get_time() + ' ' + job + '     reduce operation completed')
            logging.info(self.get_time() + ' ' + job + '     execution completed')

    #To schedule events
    def do_schedule(self, arg):
        try:
            job = arg[0:arg.index(' ')]
            delay = int(arg[arg.index(' ')+1:])
            scheduler = sched.scheduler(time.time, time.sleep)
            scheduler.enter(delay, 1, self.do_run, argument=(job,))
            logging.info(self.get_time()+' '+job+' will be executed in '+str(delay)+ ' seconds...')
            scheduler.run()
        except Exception as e:
            logging.error(self.get_time()+' '+job+ ' schedule operation failed: ' + str(e))
            return

    #To run multiple jobs at once
    def do_parallel_run(self, jobs):
        par_pool = Pool() #To parallelize job processing
        for job in jobs.split(' '):
            job = job.strip(' ')
            thread = Thread(target=self.do_run, args=(job,))
            thread.start()
        thread.join()

    def do_quit(self, arg):
        logging.info(self.get_time()+' Exiting...\nThanks for trying out this MapReduce simulation. '
                                     'Hope you liked it:)')
        raise SystemExit

    def get_parameters(self, path, job):
        with open(path + '/' + job + '.json', 'r') as fp:
            pars = json.load(fp)
        return pars

    def get_files(self, path, pars):
        if isinstance(pars['dataset'], str):
            files = [path + "/" + pars['dataset'] + "/" + f for f in os.listdir(path + "/" + pars['dataset'] + "/")
                     if f.endswith('.csv')]
        else:
            files = []
            for i in pars['dataset']:
                files.append([path + "/" + i + "/" + f for f in os.listdir(path + "/" + i + "/")
                             if f.endswith('.csv')])
        return files

    def get_time(self):
        e_time = str(datetime.datetime.now().strftime("%Y.%m.%d %H.%M"))
        return e_time

if __name__ == '__main__':
    main = Main()
    main.prompt = '>>'
    main.cmdloop('\nWelcome to MapReduce framework simulation\n'
                        '\n'
                        'Available commands:\n'
                        '\n'
                        '    run          - to run MapReduce Job (e.g. run Job1)\n'
                        '    schedule     - to schedule a job (e.g. schedule Job2 1)\n'
                        '    parallel_run - to run multiple jobs in parallel (e.g. parallel_run Job1 Job2)\n'
                        '    quit         - to exit simulation\n')
