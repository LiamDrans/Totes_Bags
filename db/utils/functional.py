''' functional programming helpers '''
from typing import Callable, Any
from functools import reduce

def compose(*fns) -> Callable:
    ''' compose functions '''
    return lambda x: reduce(lambda g,f: f(g), reversed(fns), x)

def compose_two(f: Callable, g: Callable) -> Callable:
    ''' composes two functions '''
    def map_on(x: Any) -> Any:
        return f(g(x))
    return map_on
