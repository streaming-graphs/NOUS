import matplotlib.pyplot as plt

x4 = [1, 2, 3]
y4 = [63.06, 82.83, 89.84]

x5 = [1, 2, 3]
y5 = [62.59, 81.44, 89.66]

x6 = [1, 2, 3, 4]
y6 = [59.45, 77.23, 86.21, 90.50]

x7 = [1, 2, 3, 4, 5]
y7 = [57.37, 73.83, 83.72, 88.46, 90.81]

plt.plot(x4, y4, 'r-', label='Path Len = 4')
plt.plot(x5, y5, 'b--', label='Path Len = 5')
plt.plot(x6, y6, 'g:', label='Path Len = 6')
plt.plot(x7, y7, 'k-.', label='Path Len = 7')

plt.xlabel('Entity Substitutions')
plt.ylabel('Accuracy')
plt.title('Accuracy of Model vs No. of Entity Substitutions for Varying Path Lengths')
plt.legend(loc='lower right')
plt.locator_params(axis='x', nbins=5)
plt.locator_params(axis='y', nbins=5)
plt.savefig('acc.pdf')
