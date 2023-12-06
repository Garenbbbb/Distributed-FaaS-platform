from .common import execute, verify_results, mult, concat

def test_high_volume():
  mult_test_cases = execute(
    mult, 
    lambda x:(0, x, x), 
    lambda x:{}, 
    500)
  concat_test_cases = execute(
    concat, 
    lambda x:(0, str(x)), 
    lambda x:{'b': f'{x*2}', 'c': f'{x*3}'}, 
    500)
  
  verify_results(mult_test_cases)
  verify_results(concat_test_cases)
