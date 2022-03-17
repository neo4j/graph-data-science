import numpy as np

with open("predictions.csv", 'w') as f:
    for _ in range(1000):
        y = '-' if (np.random.randn() > 0) else '+'
        proba = (np.random.randn() ) ** 2
        line = y + ',' + str(proba) + '\n'
        f.write(line)


