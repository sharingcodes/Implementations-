from multiprocessing import Process, Pool, Queue
from Parser import ReadEx, ReadDEx
from AC_PPC import ACS
import copy
import time
from numba import jit

@jit
def DisACS(n, W, own, owner, node_neighbors, external_vars, id, mailboxs, p):

    k = 0

    V_i = own[id]

    iterations = {}  # the iteration at which a external variable is

    for i in external_vars[id]:
        iterations[i] = 0

    while k <= 15:
        k += 1
        for i in V_i:

            node_neighbors_copy = copy.deepcopy(node_neighbors[i])

            if 0 in node_neighbors_copy:
                node_neighbors_copy.remove(0)

            while len(node_neighbors_copy) > 0:
                j = node_neighbors_copy.pop()
                if j not in V_i:
                    if iterations[j] < k - 1:
                        while not mailboxs[id].empty():
                            msg = mailboxs[id].get()  # a message is of the form (v_i, w0i, wi0, iter)
                            v_i = msg[0]
                            w_0i = msg[1]
                            w_i0 = msg[2]
                            iter = msg[3]
                            W[(0, v_i)] = w_0i
                            W[(v_i, 0)] = w_i0
                            iterations[v_i] = iter
                    if iterations[j] < k - 1:
                        node_neighbors_copy.add(j)
                    else:

                        a = min(W[(0, i)], W[(0, j)] + W[(j, i)])
                        b = min(W[(i, 0)], W[(i, j)] + W[(j, 0)])

                        if a + b < 0:
                            print('DACS detect inconsistency')
                            #return 0, 0

                        W[(0, i)] = a
                        W[(i, 0)] = b

                        for node in node_neighbors[i]:
                            if (node != 0) and (node not in V_i):
                                mailboxs[owner[node]].put((i, a, b, k))

                else:

                    a = min(W[(0, i)], W[(0, j)] + W[(j, i)])
                    b = min(W[(i, 0)], W[(i, j)] + W[(j, 0)])

                    if a + b < 0:
                        print('DACS detect inconsistency')
                        # return 0, 0

                    W[(0, i)] = a
                    W[(i, 0)] = b

                    for node in node_neighbors[i]:
                        if (node != 0) and (node not in V_i):
                            mailboxs[owner[node]].put((i, a, b, k))
    mailboxs[p].put((id, W))



def start_DACS(agentnum, n, W, own, owner, node_neighbors, external_vars):

    mailboxs = []

    workers = []

    for i in range(0, agentnum):
        mail_box = Queue()
        mailboxs.append(mail_box)

    mailboxs.append(Queue())  #  the p-th mailbox for main process receiving domains outputs

    for i in range(0, agentnum):
        agent = Process(target=DisACS, args=(n, W, own, owner, node_neighbors, external_vars, i, mailboxs, agentnum))  # create agent i
        workers.append(agent)
        agent.start()  # kick off agent i


    num = 0
    while True:
        msg = mailboxs[agentnum].get()
        i = msg[0]
        W_i = msg[1]

        for j in own[i]:
            W[(0, j)] = W_i[(0, j)]
            W[(j, 0)] = W_i[(j, 0)]

        num += 1

        if num == agentnum:
            break


    for i in workers:
        i.join()  # wait for agent i to exit

    print('distributed algorithm exists')

    return W


def start_DACS(agentnum, n, W, own, owner, node_neighbors, external_vars):

    mailboxs = []

    workers = []

    for i in range(0, agentnum):
        mail_box = Queue()
        mailboxs.append(mail_box)

    mailboxs.append(Queue())  #  the p-th mailbox for main process receiving domains outputs

    for i in range(0, agentnum):
        agent = Process(target=DisACS, args=(n, W, own, owner, node_neighbors, external_vars, i, mailboxs, agentnum))  # create agent i
        workers.append(agent)
        agent.start()  # kick off agent i

    num = 0
    while True:
        msg = mailboxs[agentnum].get()
        i = msg[0]
        W_i = msg[1]

        for j in own[i]:
            W[(0, j)] = W_i[(0, j)]
            W[(j, 0)] = W_i[(j, 0)]


        #print('agent {} domains received'.format(i))

        num += 1

        if num == agentnum:
            break

    for i in workers:
        i.join()  # wait for agent i to exit
    print('DisACS exists')
    return W


if __name__ == '__main__':




