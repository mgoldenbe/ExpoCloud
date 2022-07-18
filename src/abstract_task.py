from src import util

class AbstractTask:
    """
    This is a parent class for Task classes. A Task class's __init__ call it's superclass's __init__ at the end. Each task should have a set self.timeout.
    """

    class Hardness:
        """
        Each Task object has `self.hardness`, which is an instance of the Hardness class. It's instantiation depends on the existence of hardness_parameters() method in Task. 
        
        The AbstractTask.Hardness class provides default comparators. A Task class can overwrite these comparators or anyother method of AbstractTask.Hardness by subclassing it's own Hardness class from AbstractTask.Hardness.
        """
        def __init__(self, params):
            self.params = params

        def __str__(self):
            return str(self.params)

        def __repr__(self):
            return str(self)

        def __lt__(self, other):
            """
            Returns true if this task is strictly easier than the `other` task.
            The following implementation relies on the existence of the hardness_parameters() function.
            """
            return util.all_lt(self.params, other.params)
        
        def __le__(self, other):
            """
            Returns true if this task is easier than or of the same hardness as the `other` task.
            The following implementation relies on the existence of the hardness_parameters() function.
            """
            return util.all_le(self.params, other.params)
        
    def __init__(self):
        self.hardness = self.Hardness(self.hardness_parameters())
        self.result = None

    def group_parameter_titles(self):
        """
        Returns the tuples of titles of columns for parameters that determine groups for counting the number of non-hard instances.
        """
        return ()
    
    def group_parameters(self):
        """Returns the parameters that determine groups for counting the number of non-hard instances. This method should not be overrided."""
        all_titles = self.parameter_titles()
        group_titles = self.group_parameter_titles()
        indices = [i for i in range(len(all_titles)) 
                   if all_titles[i] in group_titles]

        all_params = self.parameters()
        return tuple(all_params[i] for i in indices)

    def parameter_titles(self):
        """
        Returns the tuples of titles of columns for parameters for formatted output.
        Global id and id per parameter setting are included by default and should not appear here.
        """
        return ()

    def parameters(self):
        """
        Returns the tuple of parameters. These parameters are used by the server:
        1. To determine the instance number for each parameter setting.
        2. To provide formatted output.
        Global id and id per parameter setting are included by default and should not appear here.
        """
        return ()
    
    def result_titles(self):
        """
        Returns the tuples of titles of columns for results for formatted output.
        """
        return ()
    
    def __str__(self):
        return str(self.parameters())
    
    def __repr__(self):
        """
        For easy printing of containers of tasks.
        """
        return str(self)