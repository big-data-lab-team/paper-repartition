import math

def shape_to_end_coords(M, A, d=3):
    '''
    M: block shape M=(M1, M2, M3). Example: (500, 500, 500)
    A: input array shape A=(A1, A2, A3). Example: (3500, 3500, 3500)
    Return: end coordinates of the blocks, in each dimension. Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    '''
    return [ [ (j+1)*M[i] for j in range(int(A[i]/M[i])) ] for i in range(d)]

def seeks(A, M, D):
    '''
    A: shape of the large array. Example: (3500, 3500, 3500)
    M: coordinates of memory block ends (read or write). Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    D: coordinates of disk block ends (input or output). Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    Returns: number of seeks required to write M blocks into D blocks. This number is also the number of seeks
             to read D blocks into M blocks.
    '''

    c = [ 0 for i in range(len(A))] # number of cuts in each dimension
    m = [] # number of matches in each dimension

    n = math.prod( [len(D[i]) for i in range(len(A))])  # Total number of disk blocks

    for d in range(len(A)): # d is the dimension index
        
        nd = len(D[d])
        Cd = [ ]  # all the cut coordinates (for debugging and visualization)
        for i in range(nd): # for each output block, check how many pieces need to be written
            if i == 0:
                Cid = [ m for m in M[d] if 0 < m and m < D[d][i] ]  # number of write block endings in the output block
            else:               
                Cid = [ m for m in M[d] if D[d][i-1] < m and m < D[d][i] ]  # number of write block endings in the output block
            if len(Cid) == 0:
                continue
            c[d] += len(Cid) + 1
            Cd += Cid

        m.append(len(set(M[d]).union(set(D[d]))) - c[d])

    s = A[0]*A[1]*c[2] + A[0]*c[1]*m[2] + c[0]*m[1]*m[2] + n

    return s

## Test cases

cases = { # cases used for the paper (table 1)
        "case 1_0": {
                "A": [3500,3500,3500],
                "I": [500,500,875],
                "O": [500,500,500],
                "ref": 0
        },
        "case 1_1": {
                "A": [3500,3500,3500],
                "I": [500,875,500],
                "O": [500,500,500],
                "ref": 1
        },
        "case 1_2": {
                "A": [3500,3500,3500],
                "I": [875,500,500],
                "O": [500,500,500],
                "ref": 2
        },
        "case 1_3": {
                "A": [3500,3500,3500],
                "I": [875,875,500],
                "O": [500,500,500],
                "ref": 3
        },
        "case 1_4": {
                "A": [3500,3500,3500],
                "I": [875,875,875],
                "O": [500,500,500],
                "ref": 4
        },
    }


for c in cases:
    A = cases[c]["A"]
    M = cases[c]["I"]  # for baseline, I=R=W
    D = cases[c]["O"]
    ni = int(math.prod( A[d]/M[d] for d in range(3)))
    s = seeks(A, shape_to_end_coords(M, A), shape_to_end_coords(D, A)) + ni
    print(s)
