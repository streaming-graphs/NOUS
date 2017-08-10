import matplotlib.pyplot as plt

x4 = [1, 2, 3]
y4 = [56682242.5111, 42409788.2894, 26616755.5489]
y4 = y4[::-1]

x5 = [1, 2, 3]
y5 = [46675360.4465, 35118334.7879, 22104761.1586]
y5 = y5[::-1]

x6 = [1, 2, 3, 4]
y6 = [49348360.2442, 39711093.0731, 30169737.8847, 18969282.2975]
y6 = y6[::-1]

x7 = [1, 2, 3, 4, 5]
y7 = [51379932.3634, 42940861.108, 34637213.0687, 26179730.0921, 16653866.1367]
y7 = y7[::-1]

plt.plot(x4, y4, 'r-', label='Path Len = 4')
plt.plot(x5, y5, 'b--', label='Path Len = 5')
plt.plot(x6, y6, 'g:', label='Path Len = 6')
plt.plot(x7, y7, 'k-.', label='Path Len = 7')

plt.xlabel('Entity Substitutions')
plt.ylabel('Mean(Var. of Pos. Samples ~ Var. of Neg. Samples)')
plt.title('Mean Diff. Variance vs No. of Entity Subs. for Varying Path Lengths')
plt.legend(loc='lower right')
plt.locator_params(axis='x', nbins=5)
plt.locator_params(axis='y', nbins=5)
plt.savefig('diff_var.pdf')
