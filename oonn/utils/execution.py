# -*- coding: utf-8 -*-


"""
@author: Shankar Ratneshwaran
@classes :Process Task
@filename:execution.py
@description: All execution related objects that encapsulates a list of tasks 
or steps like for preprocessing before handing the data over for a 
neural network for trainingor inference, 
later performing any post-processing steps can be defined using these classes

"""

import logging
import sys

from .counter import Counter

class Task(object) :
    """
    Base class for all Tasks that need to be executed in a Process object.  
    """    
    def __init(self):
        """
        Null Constructor initializing the object
        """
        self._task_id = 0
        self._task_name = ""
    def __init__(self, task_id,task_name = ""):
        """
        Constructor that creates a Task with a task id and name
        """
        self._task_id = task_id
        self._task_name = task_name
    def set_id(self, task_id):
        """
        Set the id of this Task
 
        Parameters
        ----------       
        task_id: to be set by the managing Process object 
        WARNING : do not set it in the user program as execution order 
        will be disturbed
     
        Returns
        -------
        None
      
        """
      
        self._task_id = task_id
    def get_id(self):
        """
        Get the id of this Task
 
        Parameters
        ----------
        None
        
        Returns
        -------
        Task id of the task added to the list
      
        """
        return self._task_id
    
    def set_name(self, task_name):
        """
        Set the name of this Task
 
        Parameters
        ----------       
        task_name : human readable name for the task

        Returns
        -------
        None
      
        """
      
        self._task_name = task_name
    def get_name(self):
        """
        Get the human readable name of this Task
 
        Parameters
        ----------
        None
        
        Returns
        -------
        Task name of the task added to the list
      
        """
        return self._task_name
    def execute(self):
        """
        Base class implementation for execute. Sub classes should have code 
        that does more than print its name and id
 
        Parameters
        ----------
        None
        
        Returns
        -------
        Task name of the task added to the list
      
        """
        logging.info("Executing Task with ID: " +str(self.get_id() ) + " and Name: "+ self.get_name() )
    
class Process(Task): 
    """
    Process object allows a set of filters or preprocessing steps to be applied 
    on the dataset before it gets handed over to an Neural Network for training
    or inference. (subclasses of Task) Later the 
    post-processing steps are executed.  
    While the above being the intent for this class, it could be used for
    other purposes
    """
    def __init__(self):
        """
        Constructor for process to initiate the list of tasks.
          Each task is contains a subclass of Task in a dictionary 
          where the key is the task_id
        
        Parameters
        ----------
        None
        
        """
        self._tasks={}
        self._task_id_counter = Counter()
        self.set_name("Process")
        self._continue_with_errors = True

    def add_task(self, task):
        """
        Add this task (subclass of class task) to list of tasks to be executed
 
        Parameters
        ----------       
        task: subclass of class oonn.task
        Task itself could embed a process so there could be a hierarchy
        of processes and sub-processes
        
        Returns
        -------
        Task id of the task added to the list
      
        """
        __task_id = 0
        if (isinstance(task, Task)):
            __task_id = self._task_id_counter.next()
            task.set_id(__task_id)
            self._tasks[__task_id]=task
        return __task_id
        
    def pop_task(self, task_id):
        """
        Removes a task from the task list and returns it if found
        
        Parameters
        ----------
        task_id: id of he task that needs to be deleted
        
        """
        __ret_task = object()
        if task_id in self._tasks:
            __ret_task = self._tasks[task_id]
            del self._tasks[task_id]
        else:
            logging.error(str(task_id)+ "not found in the task list")
        return __ret_task
    
    def execute(self):
        """
        Execute the list of tasks inside the process
        
        """
        for _i in range(self._task_id_counter.current()+1):
            if _i in self._tasks:
                current_task = self._tasks[_i]
                try:
                    current_task.execute()
                except:
                    logging.error(sys.exc_info())
                    logging.error("ERROR: Executing "+current_task.get_name()+ 
                                  " with Id "+str(current_task.get_id() ) )
                    if self._continue_with_errors:
                        continue
                    else:
                        return
                finally:
                    logging.info("Completed Execution of Task with Name: "+
                                 current_task.get_name() +" and Id: "+ str(current_task.get_id() ) )
                    
                
                
                
"""
Unit test stub for this module
"""

def main(*args, **kwargs):
    class DummyTask(Task):
        def __init__(self, task_name):
            self.set_name(task_name)
        def execute(self):
            print("Dummy task with ID: "+ str(self.get_id())+ " Name: "+self.get_name())
    
    print("Testing module oonn.exection")
    my_process = Process()

    my_process.add_task(DummyTask("Pre-Processing"))
    my_process.add_task(DummyTask("NNProcessing"))
    my_process.add_task(DummyTask("Post-Processing"))
    my_process.execute()

    print("Test Done")


if __name__ == '__main__':
    main(sys.argv)