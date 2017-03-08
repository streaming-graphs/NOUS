import numpy as np
import tensorflow as tf

def xavier_weight_init():
  """
  Returns function that creates random tensor. 
  
  """
  def _xavier_initializer(shape, **kwargs):
    """Defines an initializer for the Xavier distribution.

    This function will be used as a variable scope initializer.

    https://www.tensorflow.org/versions/r0.7/how_tos/variable_scope/index.html#initializers-in-variable-scope

    Args:
      shape: Tuple or 1-d array that species dimensions of requested tensor.
    Returns:
      out: tf.Tensor of specified shape sampled from Xavier distribution.
    """
   
    
    m = shape[0]
    n = shape[1] if len(shape) > 1 else shape[0]

    bound = np.sqrt(6) / np.sqrt(m + n)
    out = tf.random_uniform(shape, minval=-bound, maxval=bound)
    
    return out
    
  return _xavier_initializer

def test_initialization_basic():
  """
  Some simple tests for the initialization.
  """
  print "Running basic tests..."
  xavier_initializer = xavier_weight_init()
  shape = (1,)
  xavier_mat = xavier_initializer(shape)
  print xavier_mat
  assert xavier_mat.get_shape() == shape

  shape = (1, 2, 3)
  xavier_mat = xavier_initializer(shape)
  print xavier_mat
  assert xavier_mat.get_shape() == shape
  print "Basic (non-exhaustive) Xavier initialization tests pass\n"

if __name__ == "__main__":
    test_initialization_basic()
