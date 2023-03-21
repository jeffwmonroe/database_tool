class base:
    def get_name(self):
        pass


class Child(base):

    def __init__(self):
        self.age = 12
        self.name = 'Alexis'

    def get_name(self):
        print(f'Name = {self.name}')

    def get_age(self):
        print(f'Age = {self.age}')

def factory():
    return Child()
