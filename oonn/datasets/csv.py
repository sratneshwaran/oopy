# -*- coding: utf-8 -*-

"""
@author: Shankar Ratneshwaran
@classes:CSV 
@filename:csv.py
@description: Dataset download and manipulation class for csv
the _dataset member holds all the data in a pandas Dataframe
"""


from ..prep.data import InMemDataset
import numpy as np
import pandas as pd
import os

class CSV(InMemDataset):
    """
    CSV class helps with download and manipulation of CSV data.  
    This class can help with both archived and unarchived csv files 
    that could be either already downloaded in a path or not
        
    """
    def __init__(self, source_url, data_file_path, download_path="./data/",  already_downloaded=True, to_be_copied = False):
        super().__init__(
            source_url = source_url,
            training_data_filename = data_file_path
            download_path = download_path,
            already_downloaded=True
            )

        """
        Constructor that is specific to csv files.  Has simple methods that would be useful for
        other subclasses to implement or use
        
        Parameters
        ----------
        :param source_url: str
           The source URL or link where the dataset needs to be 
        downloaded from or is present. The dataset will be copied onto the ./data 
        :param data_file_path: str
            The path of the extracted (if inside an archive). If unarchived
        provide just the name of the file along with the source_url/folder name
            
        :param download_path: str
           the local folder or url where the dataset needs to be 
        downloaded and extracted
        
        already_downloaded
        :param to_be_copied: bool
            True: needs copying from source_url to download path
            False: it does not need copying
        """
        if !_needs_extraction(self._source_url):
            self._extracted = True

    def load(self):
        """
        Loads the dataset into the memory
        """
        data_url = ""
        if ! self.already_downloaded:
            """ Download extract and save to the download path NOT TESTED ///???"""
            if _isweburl(source_url)
                self.download()
                
        else:
            data_url = os.path.join(self.download_url,
                                    self._train_data_filename)
        self._dataset = pd.read_csv(data_url)
        
    def save(path):
        """

        """
