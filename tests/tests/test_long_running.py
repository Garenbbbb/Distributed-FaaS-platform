from random import randrange
from .common import execute, verify_results, mult, concat, matmul

def test_high_volume():
  mult_test_cases = execute(
    mult, 
    lambda x:(randrange(1, 2), x, x), 
    lambda x:{}, 
    5)
  concat_test_cases = execute(
    concat, 
    lambda x:(randrange(1, 2), str(x)), 
    lambda x:{'b': f'{x*2}', 'c': f'{x*3}'}, 
    5)
  matmul_test_cases = execute(
    matmul, 
    lambda x:(x,), 
    lambda x:{}, 
    5)
  
  verify_results(mult_test_cases)
  verify_results(concat_test_cases)
  verify_results(matmul_test_cases, lambda A,B: (A==B).all())
