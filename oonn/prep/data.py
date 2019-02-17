# -*- coding: utf-8 -*-

"""
@author: Shankar Ratneshwaran
@classes:Dataset 
@filename:data.py
@description: All data manipulation related objects that encapsulates 
initial data download and uncompress steps. Once downloaded the data is either
ready for loading into a serial database or loaded in memory space permitting.
The Dataset class could be used directly for some basic datasets but it is 
highly recommended that the class is extended for use
"""

import logging
import urllib.request
import tarfile
import zipfile
import shutil
import os, sys


# from  ..utils.execution import Process, Task
class Dataset(object):
    """
    Base class for all Datasets that need to be downloaded and unzipped.  
    """

    def __init__(self, source_url, download_url="./data/",
                 train_data_filename="", validate_data_filename="", test_data_filename="",
                 trail_labels_filename="", validate_labels_filename="", test_labels_filename="",
                 train_subfolder="train", validate_subfolder="validate", test_subfolder="test",
                 uncompress=True, verbose=True,
                 refresh_everytime_used=False, already_downloaded=False):

        """
        Set the id of this Task
 
        Parameters
        ----------
        source_url: str
            The source URL or link where the dataaset needs to be
        downloaded from
        download_url: str
            the local folder or url where the dataset needs to be 
        downloaded and extracted
        train_data_filename: str
            if the url has a separate file for training then mention it else
            pass the train_subfolder
                    
        validate_data_filename: str
            if the url has a separate file for validation then mention it else
            pass the validation_subfolder

        test_data_filename: str
            if the url has a separate file for testing then mention it else
            pass the testing_subfolder

        train_labels_filename: str
            if the url has a separate file for training labels then mention it else
            pass the train_subfolder

        validate_labels_filename: str
            if the url has a separate file for validation labels then mention it else
            pass the validation_subfolder

        test_labels_filename: str
            if the url has a separate file for testing labels then mention it else
            pass the testing_subfolder

        train_subfolder: str
            Once downloaded and extracted the folder where 
        the training samples will be found
        validate_subfolder: str
            Once downloaded and extracted the folder where 
        the validation samples will be found
        test_subfolder: str
            Once downloaded and extracted the folder where 
        the testing samples will be found
        
        uncompress: bool
            Value indicating whether the dataset needs 
        to be extracted from an archive after dowloading. Setting this to false
        generally means that the source_url provided has tso be ignored and 
        already_downloaded flag needs to be set to true as the dataset is 
        already dowloaded and extracted in the download_url 
        verbose: bool
            Prints a progress bar in stdout if set to True
        
        refresh_everytime_used: bool
            flag indicating if the dataset needs to
        be downloaded everytime download_if_needed() method is called. 
        Generally used when the dataset at the source_url changes frequently.
        
        already_downloaded: Indicates that the dataset is already downloaded
        and the dataset will only be refreshed only if the refresh() 
        method is called
        
        Returns
        -------
        None
      
        """
        self._source_url = source_url
        self._download_url = download_url
        self._train_filename = train_filename
        self._validate_filename = validate_filename
        self._test_filename = test_filename
        self._train_labels = train_labels_filename
        self._validate_labels = validate_labels_filename
        self._test_labels = test_labels_filename
        self._train_subfolder = train_subfolder
        self._validate_subfolder = validate_subfolder
        self._test_subfolder = test_subfolder
        self._uncompress = uncompress
        self._refresh_everytime_used = refresh_everytime_used
        self._downloaded = already_downloaded

    def _print_download_progress(self, count, block_size, total_size):
        """
        Function used as a callback in download_extract_if_needed
        Parameters
        ----------       
        count: 
        block_size:
        total_size:
        """
        # Percentage completion.
        pct_complete = float(count * block_size) / total_size

        # Limit it because rounding errors may cause it to exceed 100%.
        pct_complete = min(1.0, pct_complete)

        # Status-message. Note the \r which means the line should overwrite itself.
        msg = "\r- Progress: {0:.1%}".format(pct_complete)

        # Print it.
        sys.stdout.write(msg)
        sys.stdout.flush()

    def _check_if_downloaded(self):
        """
        Base class check whether the download folder exists and 
        sets the _downloaded flag to true.
        Subclasses will implement this by adding additional checks
        """
        if os.path.exists(self._download_url):
            self._downloaded = True

    def download_extract_if_needed(self, verbose=True):
        """
        Downloads the dataset from the source_url set in the constructor
        if not done already.
        Note:(if you set the already_dowloaded flag is set to True in the
        constructor this method will do nothing)
 
        Parameters
        ----------       
        verbose : bool
            True  - a progress indicator displayed 
            False - quiet operation
        
        
        Returns
        -------
        RetVal: bool
            True : if the dataset was downloaded and extracted
            False: 
        """
        self._check_if_downloaded()
        if not self._downloaded:
            fnames = [self._train_filename, self._validate_filename, self._test_filename]
            labels = [self._train_labels, self._validate_labels, self._test_labels]

            subfolders = [self._train_subfolder, self._validate_subfolder, self._test_subfolder]

            download_folder = ""
            for fname, label, subfolder in zip(fnames, labels, subfolders):
                if subfolder != "":
                    download_folder = os.path.join(self._download_url, subfolder)
                self._download_and_extract(download_folder, self._source_url, fname, verbose)
        self._downloaded = True

    def extract(self, extract_filepath, extract_folder):
        """
        Base class implementation Extracts zip and tar.gz or gzip files.
        Subclasses can extend by adding 
       if (extn = ".new")
            mysubclass_extract(...)
        else:
            self.extract(extract_filepath, extract_folder)

        extract_filepath: str
            full path of the archive file that needs to be extracted
        extract_folder: str
            full path of the folder where it needs to be extracted
        """
        if extract_filepath.endswith(".zip"):
            # Unpack the zip-file.
            zipfile.ZipFile(file=extract_filepath,
                            mode="r").extractall(extract_folder)
        elif extract_filepath.endswith((".tar.gz", ".tgz")):
            # Unpack the tar-ball.
            tarfile.open(name=extract_filepath,
                         mode="r:gz").extractall(extract_folder)

    def _download_and_extract(self, source_url, download_folder, filename="", verbose=True):
        """
        Downloads the dataset from the download_url into the download_folder
        To be used by the subclasses to download the files in a dataset
        
        Parameters
        ----------       
        download_folder : str
            folder where the dataset needs to be downloaded
        source_url: str
            URL where the dataset is downloaded from
        filename: str
            the file that has to be downloaded. Useful when multiple files 
            have to be downloaded. For brevity the source_url 
            could contain the filename
        
        Returns
        -------
        RetVal: bool
            True : if the dataset was downloaded and extracted
            False: 
        """
        # if a filename is not provided the URL contains the filename
        if source_url
        if filename == "":
            destn_fname = source_url[source_url.rfind("/") + 1:]
        else:
            destn_fname = filename
        dwnld_path = os.path.join(download_folder, destn_fname)

        # Check if the download directory exists, otherwise create it.
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)
        if verbose:
            print("Downloading", filename, end=" ")

        # Download the file from the internet. if a filename is provided
        # join it with the url else url contains the filename

        if filename != "":
            url_req = os.path.join(source_url, filename)
        else:
            url_req = source_url

        file_path, _ = urllib.request.urlretrieve(url=url_req,
                                                  filename=dwnld_path,
                                                  reporthook=self._print_download_progress)
        if verbose:
            print("Done!")
            print("Extracting " + dwnld_path + " ... ", end=" ")
        self.extract(dwnld_path, download_folder, verbose)

        # Extracting the files

    def refresh(self, verbose=True):
        """
        Downloads the dataset from the source_url even if it was previously 
        downloaded.
        Base class implementation cleans up the download_url passed in the 
        constructor.  Subclasses can add additional implementation
        
        """
        shutil.rmtree(self._download_url)
        self._downloaded = False
        self.download_extract_if_needed(verbose)

    class InMemDataset(Dataset):
        """
        InMemory dataset can be downloaded/extracted and loaded in memory if the size is small enough
        to fit in memory. The dataset is presented in a numpy array for convenience of other modules
        """
        def __init__(self, source_url, download_path,
                 train_data_filename="", validate_data_filename="", test_data_filename="",
                 trail_labels_filename="", validate_labels_filename="", test_labels_filename="",
                 train_subfolder="train", validate_subfolder="validate", test_subfolder="test",
                 uncompress=True, verbose=True,
                 refresh_everytime_used=False, already_downloaded=False):
            """
            Constructor that initializes the InMemDataset
            Params
            """

        def _getDataSet(self):
            return
