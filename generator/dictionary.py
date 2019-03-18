# -*- coding: utf-8 -*-

class Dictionary(object):
    def __init__(self):
        self._ids = []
        self._names = {}

    def load(self, filename):
        with open(filename, 'r') as file:
            for line in file:
                name = line.strip()
                self._names[name] = len(self._ids)
                self._ids.append(name)

    def id(self, name):
        return self._names.get(name, -1)

    def contains(self, name):
        return name in self._names

    def size(self):
        return len(self._ids)

