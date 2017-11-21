from multiprocessing import Process, Pool, Queue, Manager
from Parser import ReadEx, ReadDEx
from AC_PPC import ACS, PPC
from DisACS import start_DACS
import copy
import time
from math import inf
from numba import jit

@jit
def DPPC(agentnum, n, W, E, own, owner, node_neighbors, shared_vars, shared_ordering, id, mailboxs):

    V_id = own[id]  # local variables
    V_p = set()  # private variables
    V_s = set()  # shared variables

    for v in V_id:
        if v in shared_vars:  # shared_vars is the set of all shared variables
            V_s.add(v)
        else:
            V_p.add(v)

    # stage 1: eliminate all the private variables

    private_order = sorted(V_p)
    private_order.reverse()
    #print(private_order)
    for k in private_order:
        #print(k)
        k_neighbor = copy.deepcopy(node_neighbors[k])
        #print(k_neighbor)
        while len(k_neighbor) > 1:
            i = k_neighbor.pop()
            if i < k:
                for j in k_neighbor:
                    if j < k:
                        if j not in node_neighbors[i]:
                            node_neighbors[i].add(j)
                            node_neighbors[j].add(i)
                            E.append((i, j))
                            W[(i, j)] = inf
                            W[(j, i)] = inf

                        a = min(W[(i, j)], W[(i, k)] + W[(k, j)])
                        b = min(W[(j, i)], W[(j, k)] + W[(k, i)])

                        W[(i, j)] = a
                        W[(j, i)] = b

                        if W[(i, j)] + W[(j, i)] < 0:
                            print('DPPC stage 1 detects inconsistency')
                            return 0, 0, 0, 0

    # stage 2: eliminate all the shared variables

    Vs_eliminated = set()

    for l in range(0, len(shared_ordering)):
        k = shared_ordering[l]
        if k in V_s:
            while True:
                waiting_count = 0
                for m in range(0, l):
                    i = shared_ordering[m]
                    if (i in node_neighbors[k]) and (i not in Vs_eliminated):
                        waiting_count += 1
                # print('agent {} want to eliminate {}, waiting count is {}'.format(id, k, waiting_count))
                if waiting_count == 0:
                    #print('break')
                    break

                msg = mailboxs[id].get()

                # print(msg[0])
                if msg[0] == 'update edge':

                    i = msg[1]
                    j = msg[2]
                    w_ij = msg[3]
                    w_ji = msg[4]

                    if (j not in node_neighbors[i]) or (i not in node_neighbors[j]):
                        node_neighbors[i].add(j)
                        node_neighbors[j].add(i)
                        E.append((i, j))
                        W[(i, j)] = inf
                        W[(j, i)] = inf

                    W[(i, j)] = min(W[(i, j)], w_ij)
                    W[(j, i)] = min(W[(j, i)], w_ji)

                if msg[0] == 'eliminated':

                    Vs_eliminated.add(msg[1])

            # print('agent {} exits waiting'.format(id))

            k_neighbor = copy.deepcopy(node_neighbors[k])

            while len(k_neighbor) > 1:

                i = k_neighbor.pop()

                if (i < k) and (i not in V_p):
                    for j in k_neighbor:
                        if (j < k) and (j not in V_p):
                            if j not in node_neighbors[i]:
                                node_neighbors[i].add(j)
                                node_neighbors[j].add(i)
                                E.append((i, j))
                                W[(i, j)] = inf
                                W[(j, i)] = inf

                            a = min(W[(i, j)], W[(i, k)] + W[(k, j)])
                            b = min(W[(j, i)], W[(j, k)] + W[(k, i)])

                            W[(i, j)] = a
                            W[(j, i)] = b

                            if i != 0:
                                mailboxs[owner[i]].put(('update edge', i, j, a, b))

                            if j != 0:
                                mailboxs[owner[j]].put(('update edge', i, j, a, b))

                            if W[(i, j)] + W[(j, i)] < 0:
                                print('DPPC stage 2 detects inconsistency')

            Vs_eliminated.add(k)

            # notify all other agents that k has been eliminated

            for agent in range(0, agentnum):
                if agent != id:
                    mailboxs[agent].put(('eliminated', k))

    # stage 3: reinstated all the shared variables

    shared_ordering.reverse()

    reinstated_set = set()

    for l in range(0, len(shared_ordering)):
        k = shared_ordering[l]
        if k in V_s:
            while True:
                waiting_count = 0
                for m in range(0, l):
                    i = shared_ordering[m]
                    if (i in node_neighbors[k]) and (i not in reinstated_set):
                        waiting_count += 1
                # print('agent {} want to eliminate {}, waiting count is {}'.format(id, k, waiting_count))
                if waiting_count == 0:
                    #print('break')
                    break

                #print('stage 3: agent {} is waiting'.format(id))
                msg = mailboxs[id].get()
                #print('stage 3: agent {} exits'.format(id))

                # print(msg[0])
                if msg[0] == 'update edge':

                    i = msg[1]
                    j = msg[2]
                    w_ij = msg[3]
                    w_ji = msg[4]
                    W[(i, j)] = w_ij
                    W[(j, i)] = w_ji
                if msg[0] == 'reinstated':
                    reinstated_set.add(msg[1])

            reinstated_set_copy = copy.deepcopy(reinstated_set)

            while len(reinstated_set_copy) > 0:
                i = reinstated_set_copy.pop()
                for j in reinstated_set_copy:
                    if i in node_neighbors[k] and j in node_neighbors[k]:
                        a = min(W[(i, k)], W[(i, j)] + W[(j, k)])
                        b = min(W[(k, i)], W[(k, j)] + W[(j, i)])
                        c = min(W[(k, j)], W[(k, i)] + W[(i, j)])
                        d = min(W[(j, k)], W[(j, i)] + W[(i, k)])

                        if i != 0:
                            mailboxs[owner[i]].put(('update edge', i, k, a, b))
                        if j != 0:
                            mailboxs[owner[j]].put(('update edge', k, j, c, d))

                        W[(i, k)] = a
                        W[(k, i)] = b
                        W[(k, j)] = c
                        W[(j, k)] = d

            reinstated_set.add(k)

            for agent in range(0, agentnum):
                if agent != id:
                    mailboxs[agent].put(('reinstated', k))

    # stage 4: reinstated all the private variables

    private_order.reverse()

    for k in private_order:
        k_neighbor = copy.deepcopy(node_neighbors[k])
        while len(k_neighbor) > 1:
            i = k_neighbor.pop()
            if i < k:
                for j in k_neighbor:
                    if j < k:
                        a = min(W[(i, k)], W[(i, j)] + W[(j, k)])
                        b = min(W[(k, i)], W[(k, j)] + W[(j, i)])
                        c = min(W[(k, j)], W[(k, i)] + W[(i, j)])
                        d = min(W[(j, k)], W[(j, i)] + W[(i, k)])

                        W[(i, k)] = a
                        W[(k, i)] = b
                        W[(k, j)] = c
                        W[(j, k)] = d

    mailboxs[agentnum].put((id, W))


def start_DPPC(agentnum, n, W, E, own, owner, node_neighbors, shared_vars, shared_ordering):

    mailboxs = []
    workers = []

    for i in range(0, agentnum):
        mail_box = Queue()
        mailboxs.append(mail_box)

    mailboxs.append(Queue())  #  the p-th mailbox for main process receiving domains outputs

    for i in range(0, agentnum):
        agent = Process(target=DPPC, args=(agentnum, n, W, E, own, owner, node_neighbors, shared_vars, shared_ordering, i, mailboxs))  # create agent i
        workers.append(agent)
        agent.start()  # kick off agent i

    num = 0
    while True:
        msg = mailboxs[agentnum].get()
        i = msg[0]
        W_i = msg[1]

        for var in own[i]:
            if var != 0:
              W[(0, var)] = W_i[(0, var)]
              W[(var, 0)] = W_i[(var, 0)]
        num += 1

        if num == agentnum:
            break

    for i in workers:
        i.join()  # wait for agent i to exit

    print('DPPC exists')

    return W


if __name__ == '__main__':


