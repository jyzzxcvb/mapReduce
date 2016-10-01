########################################
## Template Code for Big Data Analytics
## assignment 1, at Stony Brook Univeristy
## Fall 2016

## Jiayao Zhang

## version 1.04
## revision history
##  .01 comments added to top of methods for clarity
##  .02 reorganized mapTask and reduceTask into "system" area
##  .03 updated reduceTask comment  from_reducer
##  .04 updated matrix multiply test

from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
import binascii

##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:#[TODO]
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=4, num_reduce_tasks=3): #[DONE]
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks

    ###########################################################
    #programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v): #[DONE]
        print ("Need to override map")


    @abstractmethod
    def reduce(self, k, vs): #[DONE]
        print ("Need to override reduce")


    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, map_to_reducer): #[DONE]
        #runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            #run mappers:
            mapped_kvs = self.map(k, v)
            #assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                map_to_reducer.append((self.partitionFunction(k), (k, v)))


    def partitionFunction(self,k): #[TODO]
        #given a key returns the reduce task to send it
        node_number = 0
        try:
            node_number = hash(k) % self.num_reduce_tasks
        except:
            print('err')
        return node_number


    def reduceTask(self, kvs, from_reducer): #[TODO]
        #sort all values for each key into a list
        #[TODO]
        dict = {}
        for line in kvs:
            key = str(line[0])
            if key not in dict:
                dict[key] = []
            dict[key].append(line[1])
        #call reducers on each key paired with a *list* of values
        #and append the result for each key to from_reducer
        #[TODO]
        for key in dict:
            from_reducer.append(self.reduce(key, dict[key]))
        return 1

    def runSystem(self): #[TODO]
        #runs the full map-reduce system processes on mrObject

        #the following two lists are shared by all processes
        #in order to simulate the communication
        #[DONE]
        map_to_reducer = Manager().list() #stores the reducer task assignment and
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
        from_reducer = Manager().list() #stores key-value pairs returned from reducers
                                        #in the form [(k, v), ...]

        #divide up the data into chunks accord to num_map_tasks, launch a new process
        #for each map task, passing the chunk of data to it.
        #hint: if chunk contains the data going to a given maptask then the following
        #      starts a process
        #      p = Process(target=self.mapTask, args=(chunk,map_to_reducer))
        #      p.start()
        #[TODO]
        ps = []

        for i in range(self.num_map_tasks):
            chunk = []
            j = i
            while j < len(self.data): #assign job to each mapper
                chunk.append(self.data[j])
                j += self.num_map_tasks
            p = Process(target=self.mapTask, args=(chunk,map_to_reducer))
            ps.append(p)
            p.start()

        #join map task processes back
        #[TODO]
        for p in ps:
            p.join()

        #print output from map tasks
        #[DONE]
        print ("map_to_reducer after map tasks complete:")
        pprint(sorted(list(map_to_reducer)))

        #"send" each key-value pair to its assigned reducer by placing each
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]
        #[TODO]
        for line in map_to_reducer:
            to_reduce_task[line[0]].append(line[1])

        #launch the reduce tasks as a new process for each.
        #[TODO]
        ps = []

        for i in range(self.num_reduce_tasks):
            p = Process(target=self.reduceTask, args=(to_reduce_task[i],from_reducer))
            ps.append(p)
            p.start()

        #join the reduce tasks back
        #[TODO]
        for p in ps:
            p.join()

        #print output from reducer tasks
        #[DONE]
        print ("map_to_reducer after map tasks complete:")
        pprint(sorted(list(from_reducer)))

        #return all key-value pairs:
        #[DONE]
        return from_reducer


##########################################################################
##########################################################################
##Map Reducers:

class WordCountMR(MyMapReduce): #[DONE]
    #the mapper and reducer for word count
    def map(self, k, v): #[DONE]
        counts = dict()
        for w in v.split():
            w = w.lower() #makes this case-insensitive
            try:  #try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()

    def reduce(self, k, vs): #[DONE]
        return (k, np.sum(vs))


class MatrixMultMR(MyMapReduce): #[TODO]
    m_row = 0
    n_col = 0
    m_col = 0
    #overload constructor to take matrix dimensions as input
    def __init__(self, data, row, col, m_col, num_map_tasks=4, num_reduce_tasks=3): #[DONE]
        self.m_row = row
        self.n_col = col
        self.m_col = m_col
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks
    #the mapper and reducer for matrix multiplication
    def map(self, k, v): #[DONE]
        pairs = []
        if k[0] == 'm':
            for i in range(self.m_row):
                pairs.append(((k[1],i),['m',k[2], v]))
        else:
            for i in range(self.n_col):
                pairs.append(((i, k[2]),['n',k[1], v]))

        return pairs

    def reduce(self, k, vs): #[DONE]
        ms = [0] * self.m_col
        ns = [0] * self.m_col
        for v in vs:
            if v[0] == 'm':
                ms[v[1]] = v[2]
            else:
                ns[v[1]] = v[2]
        ret = 0
        for i in range(self.m_col):
            ret += ms[i] * ns[i]
        return (k, ret)

##########################################################################
##########################################################################
# PART II. Minhashing
def findPrime(n):
    """ find first prime larger than n """
    ret = n+1
    if ret % 2 == 0:
        ret += 1
    found = False

    while not found:
        found = True
        for y in range(2, int(ret ** 0.5) + 1):
            if ret % y == 0:
                found = False
                break
        if not found:
            ret += 2
    return ret

def genCoeff(n, range):
    return np.random.randint(1,range, n)
def minhash(documents, k=5): #[TODO]
    #returns a minhashed signature for each document
    #documents is a list of strings
    #k is an integer indicating the shingle size
     # for generating hash functions
    signatures = None#the signature matrix

    #Shingle Each Document into sets, using k size shingles
    #[TODO]
    shingles = []
    dict = {}
    keyCount = 0 # serves as row number
    for doc in documents:
        shingle = set()
        for i in range(0, len(doc)-k+1):
            w = (doc[i:i+5]).lower()
            if w not in dict:
                dict[w] = keyCount
                shingle.add(keyCount)
                keyCount += 1
            else:
                shingle.add(dict[w])
        shingles.append(shingle)
    #Perform efficient Minhash
    #[TODO]
    n = 100 # number of hash functions
    coeffA = genCoeff(n, keyCount)
    coeffB = genCoeff(n, keyCount)
    largePrime = findPrime(keyCount*10)
    signatures = []

    signatureD = np.full((n, len(shingles)), largePrime)
    ns = 0
    for shingle in shingles:

        signature = []
        for s in shingle:
            for i in range(0,n):
                hashed = (coeffA[i] * s + coeffB[i]) % largePrime
                signatureD[i][ns] = min(signatureD[i][ns], hashed)


        for i in range(0, n):
            minSeen = largePrime
            for s in shingle:
                hashed = (coeffA[i] * s + coeffB[i]) % largePrime
                minSeen = min(minSeen, hashed)
            signature.append(minSeen)
        signatures.append(signature)
        ns += 1
    #Print signature matrix and return them
    #[DONE]
    pprint(signatures)
    pprint(signatureD)
    """
    #debugging
    for i in range(0, 2):
        # Get the MinHash signature for document i.
        signature1 = signatures[i]

        # For each of the other test documents...
        for j in range(i + 1, 3):

            # Get the MinHash signature for document j.
            signature2 = signatures[j]

            count = 0
            co = 0
            # Count the number of positions in the minhash signature which are equal.
            for k in range(0, n):
                if signature1[k] == signature2[k]:
                    count = count+1
                co = co + (signature1[k] or signature2[k])
            # Record the percentage of positions which matched.
            print  "%d and %d : %d %d %f"  % (i , j ,  count , keyCount, count / float(keyCount))
    #end debugging
    """
    #debugging
    for i in range(0, 2):

        # For each of the other test documents...
        for j in range(i + 1, 3):

            count = 0
            # Count the number of positions in the minhash signature which are equal.
            for k in range(0, n):
                if signatureD[k][i] == signatureD[k][j]:
                    count = count+1
            # Record the percentage of positions which matched.
            print  "%d and %d : %d %d %f"  % (i , j ,  count , keyCount, count / float(keyCount))
    #end debugging
    return signatures #a minhash signature for each document



##########################################################################
##########################################################################

from scipy.sparse import coo_matrix

def matrixToCoordTuples(label, m): #given a dense matrix, returns ((row, col), value), ...
    cm = coo_matrix(np.array(m))
    return  list(zip(list(zip([label]*len(cm.row), cm.row, cm.col)), cm.data))

if __name__ == "__main__": #[DONE: Uncomment peices to test]
    ###################
    ##run WordCount:
    # data = [(1, "The horse raced past the barn fell"),
    #         (2, "The complex houses married and single soldiers and their families"),
    #         (3, "There is nothing either good or bad, but thinking makes it so"),
    #         (4, "I burn, I pine, I perish"),
    #         (5, "Come what come may, time and the hour runs through the roughest day"),
    #         (6, "Be a yardstick of quality."),
    #         (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
    #         (8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted.")]
    # mrObject = WordCountMR(data, 4, 3)
    # mrObject.runSystem()

    ####################
    ##run MatrixMultiply
    #(uncomment when ready to test)
    # data1 = matrixToCoordTuples('m', [[1, 2], [3, 4]]) + matrixToCoordTuples('n', [[1, 2], [3, 4]])
    # data2 = matrixToCoordTuples('m', [[1, 2, 3], [4, 5, 6]]) + matrixToCoordTuples('n', [[1, 2], [3, 4], [5, 6]])
    # data3 = matrixToCoordTuples('m', np.random.rand(20,5)) + matrixToCoordTuples('n', np.random.rand(5, 40))
    # mrObject = MatrixMultMR(data1, 2, 2,2, 2, 2)
    # mrObject.runSystem()
    # mrObject = MatrixMultMR(data2, 2, 2, 3, 2, 2)
    # mrObject.runSystem()
    # # mrObject = MatrixMultMR(data3, 20, 40,5, 6, 6)
    # # mrObject.runSystem()

    ######################
    ## run minhashing:
    # (uncomment when ready to test)
    documents = ["The horse raced past the barn fell. The complex houses married and single soldiers and their families",
                 "There is nothing either good or bad, but thinking makes it so. I burn, I pine, I perish. Come what come may, time and the hour runs through the roughest day",
                 "Be a yardstick of quality. A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful. I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."]
    testdoc = ["Russian Prime Minister Viktor Chernomyrdin on Thursday proposed a three-phase solution to end the three-month-old conflict in Chechnya, starting with the declaration of demilitarised zones. Derek Jeter hit a go-ahead homer and finished with four hits, Alex Rodriguez added a home run and the New York Yankees beat the slumping Mets 11-8 Saturday. India has added almost 100 million people to its list of the poor, a move that will give a total of 372 million access to state welfare schemes and subsidies, a government official said Monday. Maybe a job applicant claims that he earned a bachelor's degree when he was actually one semester shy of graduation. Or he boasts of winning an award from a trade group that doesn't exist. Police on Wednesday handcuffed and led away three children and seven adults who tried to take water into the hospice where brain-damaged Terri Schiavo is being cared for. He was an important political figure, arrested for engaging in lewd conduct in a public men's Married, with children, he told no one. Instead he pleaded guilty without even hiring a lawyer, hoping the problem would quietly disappear. French judges investigating a scandal involving cash payments for airline tickets moved a step closer to President Jacques Chirac on Wednesday, questioning his daughter in the case that dates back to Chirac's time as Paris mayor. A book based on a cancer patient's diary, which has recorded the emotions of his last days on earth, is being printed and will hit the shelves soon, said Monday's China Daily.",
               "Russian Prime Minister Viktor Chernomyrdin on Thursday proposed a three-phase solution to end the three-month-old conflict in Chechnya, starting with the declaration of demilitarised zones. Derek Jeter hit a go-ahead homer and finished with four hits, Alex Rodriguez added a home run and the New York Yankees beat the slumping Mets 11-8 Saturday. India has added almost 100 million people to its list of the poor, a move that will give a total of 372 million access to state welfare schemes and subsidies, a government official said Monday. Maybe a job applicant claims that he earned a bachelor's degree when he was actually one semester shy of graduation. Or he boasts of winning an award from a trade group that doesn't exist. Police on Wednesday handcuffed and led away three children and seven adults who tried to take water into the hospice where brain-damaged Terri Schiavo is being cared for. He was an important political figure, arrested for engaging in lewd conduct in a public men's room. Married, with children, he told no one. Instead he pleaded guilty without even hiring a lawyer, hoping the problem would quietly disappear. French judges investigating a scandal involving cash payments for airline tickets moved a step closer to President Jacques Chirac on Wednesday, questioning his daughter in the case that dates back to Chirac's time as Paris mayor. A book based on a cancer patient's diary, which has recorded the emotions of his last days on earth, is being printed and will hit the shelves soon, said Monday's China Daily.",
               "South Africa's monetary authorities will follow a restrictive monetary policy in 1995, the governor of the central Reserve Bank, Chris Stals, told parliament Friday. Belgian state broadcaster RTBF said early predictions show French President Nicolas Sarkozy's conservative party will win Sunday's runoff elections for parliament. Airspace in northern Italy will be closed until Tuesday at 0600 GMT due to ash from a volcano eruption in Iceland, said Italy's civil aviation authority on Monday, revising a previous statement. Lance Berkman finally got rid of the long, unkempt locks that brought him so much grief and unwanted attention at the start of the season. But his power remained perfectly intact against the Reds. The battle between the two Ambani brothers who own India's largest private sector conglomerate, the Reliance group, is on the brink of settlement, reports said Thursday. Former Sen. Fred Thompson of Tennessee will grab most of the headlines this week as he enters the race for the White House, but Mitt Romney likes where he stands in the race for the Republican nomination. The Associated Press reported erroneously July 10 the amount that Sen. John McCain wanted stripped from security preparations for the 2002 Winter Olympics and given to the military. His amendment, which failed, would have shifted $30 million, not $60 million. China's special economic zones hosted a grand gathering to mark the 20th anniversary of their founding, Tuesday in Shenzhen City, one of the zones in south China's Guangdong Province."]
    sigs = minhash(documents, 5)
