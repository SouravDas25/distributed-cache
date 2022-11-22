from typing import List, Callable, Any


class CommonUtil:

    @staticmethod
    def findIndex(indexable: List[Any], matcher: Callable[[Any], bool]):
        for i in range(len(indexable)):
            if matcher(indexable[i]):
                return i
        return None
