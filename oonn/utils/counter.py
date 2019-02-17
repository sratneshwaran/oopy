# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
@author: Shankar Ratneshwaran
@class:counter
@filename:counter.py
@description: a simple counter class that returns the next integer
  
"""
class Counter(object):
    """
    Simple counter object that returns the next integer
    
    """
    def __init__(self):
        """
        Null Constructor that initializes the count inside the object
        
        """        
        self._count = 0
        
    def next(self):
        """
        Automatically increments the count inside the object and returns it
        
        """          
        self._count = self._count + 1
        return self._count
    def current(self):
        """
        Returns the current value of the counter without changing state
        """
        return self._count