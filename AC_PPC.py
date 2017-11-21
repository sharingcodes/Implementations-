
from numba import jit

from math import inf
from Parser import ReadEx, ReadDEx
import copy
import time


@jit
def DPC(n, W, E, node_neighbor):
    count = 0

    for k in range(n, 1, -1):

        k_neighbor = copy.deepcopy(node_neighbor[k])
        while len(k_neighbor) > 1:
            i = k_neighbor.pop()
            if i < k:
                for j in k_neighbor:
                    if j < k:
                        if j not in node_neighbor[i]:
                            node_neighbor[i].add(j)
                            node_neighbor[j].add(i)
                            E.append((i, j))
                            W[(i, j)] = inf
                            W[(j, i)] = inf

                        count += 1

                        a = min(W[(i, j)], W[(i, k)] + W[(k, j)])
                        b = min(W[(j, i)], W[(j, k)] + W[(k, i)])

                        W[(i, j)] = a
                        W[(j, i)] = b

                        if W[(i, j)] + W[(j, i)] < 0:
                            print('DPC detect inconsistency')
                            return 0, 0, 0, 0

    return W, E, count, node_neighbor


@jit
def PPC(n, W, E, node_neighbor):

    print('running PPC')

    W, E, count, node_neighbor = DPC(n, W, E, node_neighbor)

    if W == 0:
        return 0, 0

    for k in range(2, n+1, 1):
        k_neighbor = copy.deepcopy(node_neighbor[k])
        while len(k_neighbor) > 1:
            i = k_neighbor.pop()
            if i < k:
                for j in k_neighbor:
                    if j < k:
                        count += 2
                        a = min(W[(i, k)], W[(i, j)]+W[(j, k)])
                        b = min(W[(k, i)], W[(k, j)]+W[(j, i)])
                        c = min(W[(k, j)], W[(k, i)]+W[(i, j)])
                        d = min(W[(j, k)], W[(j, i)]+W[(i, k)])

                        W[(i, k)] = a
                        W[(k, i)] = b
                        W[(k, j)] = c
                        W[(j, k)] = d
    return W, count


def ACS_test_self_constructed_instance(n, W, E):
    Q = set()
    count = 0

    node_neighbor = {}
    for i in range(1, n+1):
        node_neighbor[i] = set()
        for j in range(1, n+1):
            if ((i, j) in E) or ((j, i) in E):
                node_neighbor[i].add(j)

    print('running ACS')
    loop = 0
    while len(Q) != n:
        loop += 1
        for i in range(1, n+1):
            for j in node_neighbor[i]:
                count += 1
                a = min(W['w'+str(0)+','+str(i)],W['w'+str(0)+','+str(j)]+W['w'+str(j)+','+str(i)])
                b = min(W['w'+str(i)+','+str(0)],W['w'+str(i)+','+str(j)]+W['w'+str(j)+','+str(0)])

                if a + b < 0:
                    print('ACS detect inconsistency')
                    print(loop)
                    return 0, 0

                if (a == W['w'+str(0)+','+str(i)]) and (b == W['w'+str(i)+','+str(0)]):
                    Q.add(i)
                else:
                    if i in Q:
                        Q.remove(i)
                    W['w'+str(0)+','+str(i)] = a
                    W['w'+str(i)+','+str(0)] = b
    print(loop)
    return W, count


@jit
def ACS(n, W, E, node_neighbor):
    Q = set()
    count = 0

    print('running ACS')

    loop = 0

    while len(Q) != n:
        loop += 1
        for i in range(1, n+1):
            for j in node_neighbor[i]:
                if j != 0:
                    count += 1
                    a = min(W[(0, i)],W[(0, j)]+W[(j, i)])
                    b = min(W[(i, 0)],W[(i, j)]+W[(j, 0)])

                    if a + b < 0:
                        print('ACS detect inconsistency')
                        return 0, 0

                    if (a == W[(0, i)]) and (b == W[(i, 0)]):
                        Q.add(i)
                    else:
                        if i in Q:
                            Q.remove(i)
                        W[(0, i)] = a
                        W[(i, 0)] = b
    print('ACS iteration:', loop)
    return W, count



if __name__ == '__main__':




