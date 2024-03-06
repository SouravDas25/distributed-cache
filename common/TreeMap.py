from pytreemap import TreeMap


class TreeDict(TreeMap):

    def __init__(self):
        super(TreeDict, self).__init__()


if __name__ == "__main__":
    d = TreeDict()
    for i in range(1, 15, 2):
        d.put(i, "TEMP")
    print(d)

    print(d.floor_key(3))
    print(d.lower_key(3))
