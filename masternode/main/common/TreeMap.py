from pytreemap import TreeMap

from typing import TypeVar, Generic

T = TypeVar('T')


class TreeDict(TreeMap, Generic[T]):

    def __init__(self):
        super(TreeDict, self).__init__()


if __name__ == "__main__":
    bst = TreeDict()

    for i in range(0, 150, 5):
        bst.put(i, f"Value {i}")

    print(bst)
    # {0=Value 0, 5=Value 5, 10=Value 10, 15=Value 15 ... }

    print(bst.get(50))
    # Value 50

    print(bst.lower_key(10))
    # Value 5

    print(bst.higher_key(10))
    # Value 15

    print(bst.lower_entry(6))
    # 5=Value 5

    print(bst.higher_entry(6))
    # 10=Value 10
