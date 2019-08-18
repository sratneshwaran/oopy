from ..prep.data import InMemDataset
class MNISTDataset(InMemDataset):
    def __init__(source_url = "https://storage.googleapis.com/cvdf-datasets/mnist/",
                  download_path= "./data/MNIST", ):
        super().__init__(
            source_url = source_url,
            )
        """
        Constructor that initializes all the internal variables some default
        """
