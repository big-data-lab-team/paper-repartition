from enum import Enum

class Axes(Enum):
    i: 0,
    j: 1,
    k: 2

def compute_zones(B, O):
    """ Calcule les zones pour le dictionnaire "array"
    Arguments:
    ----------
        B: buffer shape
    """

    # for each buffer
    for buffer_index in range(nb_buffers):
        _3d_index = get3dpos(buffer_index)
        T = list()
        for i in range(3):
            C = (_3d_index[i] * B[i]) % O[i]
            T.append(B[i] - C)

        # compute volumes' positions following pillow indexing for rectangles
        volumes = {
            1: [(B[Axes.i], 0, T[Axes.k]), (0, T[Axes.j], B[Axes.k])],
            2: [(B[Axes.i], T[Axes.j], T[Axes.k]), (0, B[Axes.j], B[Axes.k])],
            3: [(T[Axes.i], T[Axes.j], 0), (0, B[Axes.j], T[Axes.k])],
            4: [(B[Axes.i], 0, 0), (T[Axes.i], T[Axes.j], T[Axes.k])],
            5: [(B[Axes.i], 0, T[Axes.k]), (T[Axes.i], T[Axes.j], B[Axes.k])],
            6: [(B[Axes.i], T[Axes.j], 0), (T[Axes.i], B[Axes.j], T[Axes.k])],
            7: [(B[Axes.i], T[Axes.j], T[Axes.k]), (T[Axes.i], B[Axes.j], B[Axes.k])]
        }

        # compute hidden output files positions (in F0)
        l = list()
        for dim in range(3):
            values_in_dim = list()
            nb_hidden_files = T[dim]/O[dim]
            nb_complete = floor(nb_hidden_files)

            a = T[dim]
            values_in_dim.append(a)
            for _ in range(nb_complete):
                b = a - O[dim]
                values_in_dim.append(b)
                a = b
            if not 0 in values_in_dim:
                values_in_dim.append(0)
            values_in_dim.sort()
            l.append(values_in_dim)

        hiddenvolumes = list()
        p1 = [0,0,0]
        p2 = [1,1,1]
        for i in range(len(l[0])-1):
            for j in range(len(l[1])-1):
                for k in range(len(l[2])-1):
                    coords = [(l[0][p1[0]], l[1][p1[1]], l[2][p1[2]]),
                                    (l[0][p2[0]], l[1][p2[1]], l[2][p2[2]])]
                    hiddenvolumes.append(coords)
                    p1[Axis.k] += 1
                    p0[Axis.k] += 1
                p1[Axis.j] += 1
                p0[Axis.j] += 1
            p1[Axis.i] += 1
            p0[Axis.i] += 1

        # Add offset: process positions to get real positions in reconstructed image

        # si le volume doit être kept alors fusionner
