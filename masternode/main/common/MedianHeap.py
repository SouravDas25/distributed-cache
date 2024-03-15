import heapq


class MaxHeap:
    def __init__(self, array=[]):
        self.heap = array
        self.heapify()

    def heapify(self):
        self.heap = list(map(lambda x: -x, self.heap))
        heapq.heapify(self.heap)

    def push(self, item):
        heapq.heappush(self.heap, -item)

    def pop(self):
        popped = heapq.heappop(self.heap)
        return -popped

    def __len__(self):
        return len(self.heap)


class MinHeap:

    def __init__(self, array=[]):
        self.heap = array
        self.heapify()

    def heapify(self):
        heapq.heapify(self.heap)

    def push(self, item):
        heapq.heappush(self.heap, item)

    def pop(self):
        popped = heapq.heappop(self.heap)
        return popped

    def __len__(self):
        return len(self.heap)


class MedianHeap:

    def __init__(self, array=[]):
        self.array = array

    def push(self, item):
        self.array.append(item)
        self.array.sort()

    def getMidKey(self, fromHash, toHash) -> float:
        if fromHash is None:
            fromHash = 0
        if toHash is None or toHash == 0:
            toHash = self.array[-1]
        fr = self.array.index(fromHash)
        t2 = self.array.index(toHash)
        range = self.array[fr:t2]
        if len(range) <= 1:
            return None
        # self.array.sort() // not required
        midKey = range[len(range) // 2]
        if fromHash < midKey < toHash:
            return midKey
        raise Exception(f"Invalid key for mid key {midKey}, {fromHash}, {toHash}")

    def getMedian(self) -> float:
        return self.array[len(self.array) // 2]

