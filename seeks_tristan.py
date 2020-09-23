def get_output_seeks(A, I, R, W, O):
    '''
    A: shape of complete array (A0, A1, A2)
    R: shape of read buffers (R0, R1, R2)
    W: shape of write buffers (W0, W1, W2)
    O: shape of output files (O0, O1, O2)
    '''

    n = 3  # dimension of the array
    c = [ 0 for i in range(n)] # number of cuts in each dimension
    m = [] # number of matches in each dimension

    no = 1  # Total number of output blocks
    for d in range(n):
        no *= A[d]/O[d]
    ni = 1  # Total number of input blocks
    for d in range(n):
        ni *= A[d]/I[d] 

    for d in range(n): # d is the dimension index
        assert(A[d] % W[d] == 0) # Ad needs to be a multiple of Wd
        assert(A[d] % O[d] == 0) # Ad needs to be a multiple of Od
        nw = int(A[d]/W[d]) # number of write blocks in dimension d
        no = int(A[d]/O[d]) # number of output blocks in dimension d

        Wd = [ i*W[d] for i in range(1, nw+1)] # coordinates of the write block endings
        Od = [ i*O[d] for i in range(1, no+1)] # coordinates of the output block endings
        Cd = [ ]  # all the cut coordinates (for debugging and visualization)
        for i in range(no): # for each output block, check how many pieces need to be written
            oi = i*O[d] # beginning of the block
            Cid = [ w for w in Wd if oi < w and w < oi+O[d] ]  # number of write block endings in the output block
            if len(Cid) == 0:
                continue
            c[d] += len(Cid) + 1
            Cd += Cid

        m.append(len(set(Wd).union(set(Od))) - c[d])

    s = A[0]*A[1]*c[2] + A[0]*c[1]*m[2] + c[0]*m[1]*m[2] + no + ni

    return s

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
    s = get_output_seeks(cases[c]["A"],
                         cases[c]["I"], cases[c]["I"],
                         cases[c]["I"], cases[c]["O"])
    print(s) 
