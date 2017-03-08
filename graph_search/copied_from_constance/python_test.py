#!/usr/bin/python
import io, sys

def test_scala(input):
  v = int(input.strip())
  #print("Input :", v)
  if(input == 0):
    return (100, 200)
  elif(input == 1):
    return (300, 400)
  else:
    return (500, 600)

if __name__ == "__main__":
  #print("Number of arguments = ", len(sys.argv))
  if(len(sys.argv) == 2):
    v = test_scala(sys.argv[1])
    print(v)
  else:
    print("expecting python testpy $nodeid")


