''' functional programming helpers '''
from typing import Callable
from functools import reduce

def pipe(*fns) -> Callable:
    ''' pipe functions '''
    return lambda x: reduce(lambda f,g: g(f), fns, x)

def compose(*fns) -> Callable:
    ''' compose functions '''
    return pipe(*reversed(fns))
