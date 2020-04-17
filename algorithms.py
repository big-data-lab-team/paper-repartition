from enum import Enum

class Axes(Enum):
    i: 0,
    j: 1,
    k: 2


class Volume:
    def __init__(self, index, p1, p2):
        self.index = index
        self.p1 = p1
        self.p2 = p2


def get_main_volumes(B, T):
    """ I- Get a dictionary associating volume indices to volume positions in the buffer.
    Indexing following the keep algorithm indexing in storage order.
    Position following pillow indexing for rectangles i.e. (bottom left corner, top right corner)

    Arguments:
    ----------
        B: buffer shape
        T: Theta prime shape -> Theta value for C_x(n) (see paper)
    """
    return [
        Volume(1,
               (B[Axes.i], 0, T[Axes.k]),
               (0, T[Axes.j], B[Axes.k])),
        Volume(2,
               (B[Axes.i], T[Axes.j], T[Axes.k]),
               (0, B[Axes.j], B[Axes.k])),
        Volume(3,
               (T[Axes.i], T[Axes.j], 0),
               (0, B[Axes.j], T[Axes.k])),
        Volume(4,
               (B[Axes.i], 0, 0),
               (T[Axes.i], T[Axes.j], T[Axes.k])),
        Volume(5,
               (B[Axes.i], 0, T[Axes.k]),
               (T[Axes.i], T[Axes.j], B[Axes.k])),
        Volume(6,
               (B[Axes.i], T[Axes.j], 0),
               (T[Axes.i], B[Axes.j], T[Axes.k])),
        Volume(7,
               (B[Axes.i], T[Axes.j], T[Axes.k]),
               (T[Axes.i], B[Axes.j], B[Axes.k]))]


def compute_hidden_volumes():
    """ II- compute hidden output files' positions (in F0)
    """
    # a) get volume points
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

    # b) compute volumes' positions from volume points
    p1 = [0,0,0]
    p2 = [1,1,1]
    index = 7
    for i in range(len(l[0])-1):
        for j in range(len(l[1])-1):
            for k in range(len(l[2])-1):
                corners = [(l[0][p1[0]], l[1][p1[1]], l[2][p1[2]]),
                           (l[0][p2[0]], l[1][p2[1]], l[2][p2[2]])]
                index += 1
                volumes.append(Volume(index, corners[0], corners[1]))
                p1[Axis.k] += 1
                p0[Axis.k] += 1
            p1[Axis.j] += 1
            p0[Axis.j] += 1
        p1[Axis.i] += 1
        p0[Axis.i] += 1


def add_offsets(volumes, buffer_offset):
    """ III - Add offset to volumes positions to get positions in the reconstructed image.
    """

def get_array_dict():
    """ IV - Assigner les volumes à tous les output files, en gardant référence du type de volume que c'est
    """
    array_dict = dict()
    alloutfiles = list(range(nb_outfiles))
    for buffer_index in buffers:
        crossed_outfiles = get_crossed_outfiles(O, buffer_index)
        buffervolumes = volumes_dictionary[buffer_index]
        for volume in buffervolumes:
            for outfile in crossed_outfiles:
                if included_in(outfile, volume):
                    add_to_array_dict(outfile, volume)
                    break # a volume can belong to only one output file


def merge_cached_volumes():
    """ V - Pour chaque output file, pour chaque volume, si le volume doit être kept alors fusionner
    """
    for outfileindex in array_dict.keys():
        volumes = array_dict[outfileindex]
        for volume in volumes:
            if volume.index in volumestokeep:
                merge_volumes(volumes, volume.index)
        array_dict[outfileindex] = map_to_slices(volumes)


def compute_zones(B, O):
    """ Calcule les zones pour le dictionnaire "array"

    Arguments:
    ----------
        B: buffer shape
        O: output file shape
    """
    volumes_dictionary = dict()

    for buffer_index in range(nb_buffers):
        _3d_index = get3dpos(buffer_index)
        T = list()
        for i in range(3):
            C = (_3d_index[i] * B[i]) % O[i]
            T.append(B[i] - C)

        volumes = get_main_volumes()
        hidden_volumes = compute_hidden_volumes(volumes)
        volumes.extends(hidden_volumes)
        add_offsets(volumes, buffer_offset)
        volumes_dictionary[buffer_index] = volumes

    array_dict = get_array_dict()
    regions_dict = deepcopy(array_dict)
    offsets = get_offsets()
    regions_dict = remove_offset(regions_dict, offsets)
