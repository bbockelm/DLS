#!/usr/bin/env python
#
# 17-February-2006 Fabio Farina
#

# Parameters file: put here the test case configuration parameters

# general CLI and client parameters
threadNum = 1
#clientDir = './dummy_dls/'
clientDir = '../../LFCClient/'

# attribute for action2command list expansion
nBlocks = 4
nReplicas = 1

# paretoK stadns for the min time of wait
paretoK = 1
paretoAlpha = 1.5

# magnitude test coefficients (coeff * original qty)
magThread = 10
magBlocks = 50
magReplicas = 100
#change them for production tests
magParetoK = 1.1 

magBlockPreambleLen = 10
magSePreambleLen = 10

randRepetitions = 5
randFactor = 3

# user model action sequence upper bound
userBound = 10
# transposed Markov chain for the user simulatio model
# arrays for add, add-tr, del, get, dli, lcg
userMarkov = [
             [0.1, 0.15, 0.05, 0.7/3, 0.7/3, 0.7/3],
             [0.01, 0.15, 0.05, 0.79/3, 0.79/3, 0.79/3],
             [0.075, 0.075, 0.05, 0.8/3, 0.8/3, 0.8/3],
             [0.02, 0.02, 0.01, 0.95, 0, 0],
             [0.02, 0.02, 0.01, 0, 0.95, 0],
             [0.02, 0.02, 0.01, 0, 0, 0.95]
             ]
# initial status probability (add, add-tr, del, get, dli, lcg)
userInitStateP = [0.1/3, 0.1/3, 0.1/3, 0.9/3, 0.9/3, 0.9/3]
