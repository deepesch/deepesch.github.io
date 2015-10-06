#!/usr/bin/env python

from math import hypot
import numpy as np

class Robot(object):
    """Robot
    Welcome our new overloads!!!
    """
    def __init__(self, name, n_arms):
        self.name = name
        self.n_arms = n_arms
        self.location = 0
        self.basket = []

    def move(self, direction, steps):
        if direction == 'forward':
            self.location += steps
        elif direction == 'backward':
            self.location -= steps

    def pickup(self, item):
        self.basket.append(item)

class RobotAdvanced(Robot):
    """Robot, built even better.
    """
    def __init__(self, name, n_arms):
        self.location = [0, 0]

    def move(self, direction, steps):
        if direction == 'forward':
            self.location[1] += steps
        elif direction == 'backward':
            self.location[1] -= steps
        elif direction == 'left':
            self.location[0] -= steps
        elif direction == 'right':
            self.location[0] += steps

    def distance_from_origin(self, distance_type):
        if distance_type == 'L1':
            return np.abs(self.location[0] + self.location[1])
        elif distance_type == 'L2':
            return round(hypot(self.location[0], self.location[1]))
