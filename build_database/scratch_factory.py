
##############################################
#          Factory Practice

class Base:
    def __init__(self):
        print("building the class")
        pass

    def __del__(self):
        print("destroying the class")
        pass


base = Base()
