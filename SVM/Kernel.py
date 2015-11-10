import numpy.linalg as la
from math import exp
import numpy as np


class Kernel(object):
    """Implements a list of kernels from
    http://en.wikipedia.org/wiki/Support_vector_machine
    """
    @staticmethod
    def linear(): #linear
        def f(x, y):
            return np.inner(x, y)
        return f

    @staticmethod
    def gaussian(sigma): #Guassian
        def f(x, y):
            if sigma > 0:
                return (-sigma * (abs(x - y))**2)
        return f

    @staticmethod
    def _polykernel(dimension, offset): #polykernal
        def f(x, y):
            return offset + np.inner(x, y) ** dimension
        return f

    @staticmethod
    def inhomogenous_polynomial(dimension): #Polynomial (inhomogeneous)
        def f(x, y):
            return (np.inner(x, y) + 1) ** dimension
        return f

    @staticmethod
    def homogenous_polynomial(dimension): #Polynomial (homogeneous)
        def f(x, y):
            if dimention > 1:
                return (np.inner(x, y))**2
            else:
                raise ValueError
        return f

    @staticmethod
    def hyperbolic_tangent(kappa, c): #hyperbolic tangent
        def f(x, y):
            kx = kappa * x
            return np.tanh(np.inner(kx, out1)) + c
        return f
