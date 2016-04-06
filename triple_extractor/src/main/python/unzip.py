#!/usr/bin/python
import os

files = os.listdir('.')

for f in files:
  if f.find('.xml') != -1:
    os.remove(f)
  elif f.find('.zip') != -1:
    print('Uncompressing ' + f + ' ...')
    dir = f.replace('.zip', '')
    os.system('mkdir ' + dir)
    os.system('mv ' + f + ' ' + dir + '/')
    os.system('cd ' + dir + '; tar -xzf ' + f + '; mv ' + f + ' ..')
