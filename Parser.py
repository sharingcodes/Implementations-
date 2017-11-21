from math import inf
import time
import os
import copy
import pickle

def ReadEx(path):
    varnum = 0
    connum = 0
    W = {} # dictionary
    E = set() # list
    with open(path, 'r') as f:
        for line in f:
            content = line.split()
            if len(content) > 0:
                if content[0] == 'p':
                    varnum = int(content[2])
                    connum = int(content[3])
                if content[0] == 'a':
                    i = int(content[1])
                    j = int(content[2])
                    if content[3] == 'inf':
                        content[3] = inf
                    else:
                        content[3] = int(content[3])
                    W[(i, j)] = content[3]
                    if ((i, j) not in E) and ((j, i) not in E):
                        E.add((i, j))

    E.add((0, 1))

    W[(0, 1)] = 0  # set the first variable equal to the zero time point
    W[(1, 0)] = 0

    for i in range(2, varnum+1, 1):
        E.add((0, i))
        W[(0, i)] = inf
        W[(i, 0)] = inf

    node_neighbor = {}
    for i in range(0, varnum+1):
        node_neighbor[i] = set()
        for j in range(0, varnum+1):
            if ((i, j) in E) or ((j, i) in E):
                node_neighbor[i].add(j)

    return varnum, connum, W, E, node_neighbor


def parseEachFileCen(filepath):
    print('Parsing', filepath)
    pathDir = os.listdir(filepath)
    pathDirProcessed = filepath + 'processed'
    isExists = os.path.exists(pathDirProcessed)

    if not isExists:
       os.makedirs(pathDirProcessed)

    for allDir in pathDir:
        child = os.path.join('%s%s' % (filepath, allDir))
        isFolder = os.path.isdir(child)
        if not isFolder:
            #print(child)
            varnum, connum, W, E, node_neighbor = ReadEx(child)
            #print(W)
            #print(pathDirProcessed + '/' + allDir.rstrip('.dimacs') + '.pkl')
            with open(pathDirProcessed + '/' + allDir.rstrip('.dimacs') + '.pkl', 'wb') as f:
                pickle.dump(varnum, f, True)
                pickle.dump(connum, f, True)
                pickle.dump(W, f, True)
                pickle.dump(E, f, True)
                pickle.dump(node_neighbor, f, True)
        #break


def ReadDEx(path):
    varnum = 0
    connum = 0
    agentnum = 0
    W = {}  # weights
    E = set()  # edges
    labels = {}
    own = {}  # the set of vars owned by an agent
    agent_neighbors = {}  # the set of neighbors of an agent
    node_neighbors = {}  # the set of neighbors of a node in the constraint graph
    owner = {}  # the agent who owns the var
    external_vars = {}  # the set of external variables of an agent
    with open(path, 'r') as f:
        for line in f:
            content = line.split()
            if len(content) > 0:
                if content[0] == 'c' and content[1] == '<label>':
                    labels[content[3]] = int(content[2])
                if content[0] == 'c' and content[1] == '<own>':
                    if int(content[2]) not in own.keys():
                        own[int(content[2])] = set()
                    own[int(content[2])].add(labels[content[3]])
                    owner[labels[content[3]]] = int(content[2])
                if content[0] == 'p' and content[1] == 'sp':
                    varnum = int(content[2])
                    connum = int(content[3])
                if content[0] == 'a':
                    i = int(content[1])
                    j = int(content[2])
                    if content[3] == 'inf':
                        content[3] = inf
                    else:
                        content[3] = int(content[3])
                    W[(i, j)] = content[3]
                    if ((i, j) not in E) and ((j, i) not in E):
                        E.add((i, j))
                if content[0] == 'c' and content[1] == '<num_agents>':
                    agentnum = int(content[2])

    E.add((0, 1))

    W[(0, 1)] = 0  # set the first variable equal to the zero time point
    W[(1, 0)] = 0

    for i in range(2, varnum+1, 1):
        E.add((0, i))
        W[(0, i)] = inf
        W[(i, 0)] = inf

    for i in range(0, varnum+1):
        if i not in node_neighbors.keys():
            node_neighbors[i] = set()
        for j in range(0, varnum+1):
            if ((i, j) in E) or ((j, i) in E):
                node_neighbors[i].add(j)

    for i in range(0, agentnum):
        if i not in agent_neighbors.keys():
            agent_neighbors[i] = set()
        if i not in external_vars.keys():
            external_vars[i] = set()
        for node in own[i]:
            for node_ne in node_neighbors[node]:
                if (node_ne != 0) and (node_ne not in own[i]):
                    external_vars[i].add(node_ne)
                    agent_neighbors[i].add(owner[node_ne])

    return varnum, connum, agentnum, W, E, own, owner, node_neighbors, agent_neighbors, external_vars


def parseEachFileDis(filepath):
    print('Parsing', filepath)
    pathDir = os.listdir(filepath)
    pathDirProcessed = filepath + 'processed'
    isExists = os.path.exists(pathDirProcessed)

    if not isExists:
       os.makedirs(pathDirProcessed)

    for allDir in pathDir:
        child = os.path.join('%s%s' % (filepath, allDir))
        isFolder = os.path.isdir(child)
        if not isFolder:
            varnum, connum, agentnum, W, E, own, owner, node_neighbors, agent_neighbors, external_vars = ReadDEx(child)
            with open(pathDirProcessed + '/' + allDir.rstrip('.dimacs') + '.pkl', 'wb') as f:
                pickle.dump(varnum, f, True)
                pickle.dump(connum, f, True)
                pickle.dump(agentnum, f, True)
                pickle.dump(W, f, True)
                pickle.dump(E, f, True)
                pickle.dump(own, f, True)
                pickle.dump(owner, f, True)
                pickle.dump(node_neighbors, f, True)
                pickle.dump(agent_neighbors, f, True)
                pickle.dump(external_vars, f, True)


if __name__ == '__main__':

    # path = './ws-task-problems/TercioTestProblem16,80,0.dimacs'
    #
    # varnum, connum, agentnum, W, E, own, owner, node_neighbor, agent_neighbors, \
    # external_vars, parsing_time = ReadDEx(path)
    #
    # print(W)

    parseEachFileCen('./ny/')
    parseEachFileCen('./sf-fixedNodes/')
    parseEachFileCen('./sf-varNodes/')
    parseEachFileDis('./ws-task-problems/')
    parseEachFileDis('./ws-agent-problems/')
    parseEachFileDis('./bdh-agent-problems/')
    parseEachFileDis('./bdh-excon-problems/')





























