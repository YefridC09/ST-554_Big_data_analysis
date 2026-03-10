#SLR_slope_simulator

#ST-554

# Yefrid Cordoba
import matplotlib.pyplot as plt
import numpy as np
from numpy.random import default_rng
from sklearn import linear_model
class SLR_slope_simulator:
    """
    """
#here we initialize what the variables are needed to create each instance of the class
    def __init__(self, beta_0 : float, beta_1: float, x, sigma : float, seed : int):
        
        self.beta_0 = beta_0
        self.beta_1 = beta_1
        self.x = x
        self.sigma = sigma
        self.seed = seed 
        self.rng = default_rng(seed) #also other attributes are defined for its use 
        self.n = len(x) 
        self.slopes = []
        
#The methods for each of the calculation steps are created using the def function
#Will create the data set that is going to be used for the linear regression
#The error comes from a normal distribution with a user define variance (sigma)
    def generate_data(self): 
        y = self.beta_0 + self.beta_1 * self.x + self.rng.normal(0, self.sigma, size = self.n)
        return self.x, y
#This method uses the linear regression function to fit the created data and obtain the slope of the linear regression
    def fit_slope(self, y):
        reg = linear_model.LinearRegression()
        fit = reg.fit(self.x.reshape(-1, 1), y)
        return fit.coef_[0]
#Uses the two previous methods and iterate a user defined number of times to get slopes that are added to the list already created
    def run_simulation(self, sim : int):
        for i in range(sim):
            x, y = self.generate_data()
            s = self.fit_slope(y)
            self.slopes.append(s)
        return None
#Creates a distribution plot for the slopes
    def plot_sampling_distribution(self):
        if len(self.slopes) == 0:
            print("run_simulation must be called first")
        else:
            plt.hist(self.slopes)
            plt.show()
        return None
# Now using the data generated from the slopes we create a method to calculate 
#an especified value for the slope, and what method is used for this purpuse
    def find_prob(self, Val : float, sided : str):
        if len(self.slopes) == 0:
            print("run_simulation method must be called first")
            return None
        else:
            slopes = np.asarray(self.slopes)
            if sided == "above":
                bool_prob = slopes > Val
                return bool_prob.mean()
            elif sided == "below":
                bool_prob = slopes < Val
                return bool_prob.mean()
            elif sided == "two-sided":
                if Val > np.median(slopes):
                    bool_prob = slopes > Val
                    return 2*bool_prob.mean()
                else:
                    bool_prob = slopes < Val
                    return 2*bool_prob.mean()
