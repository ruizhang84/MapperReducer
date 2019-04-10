# CSCI-B534-DISTRIBUTED-SYSTEMS-A1
CSCI-B 534 - DISTRIBUTED SYSTEMS
## Applications
* Word-count  (app_word_count.py)
* Inverted index  (app_inverted_index.py)

To run an example of program, simply run "python app_count_word.py" or "python app_inverted_index.py", the results will
be shown in "result.txt"

## Fault-tolerance
* Mapper (test_mapper_client.py)
* Reducer (test_reducer_client.py)
* Master (test_master_server.py)

To run an example of test on fault-tolerance, it needs to run mapper/reducer and master seperately. 
The killing of any of mapper/reducer will not effect the completion of computation task (at least in most cases).
The program run a scheduler that schedule the tasks on available mapper/reducers. As long as master is not
killed, this scheduler should run all the time.

To run the test, run mapper, reducer, master separately on
each terminals ("python test_mapper_client.py", etc). The test
itself contains line and calls "random_kill_mapper" or "random_kill_reducer"
that terminates the process randomly. The program however should still run.  

## Report
Please see Assignment_1.pdf