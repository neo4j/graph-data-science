with open('predictions.csv', 'r') as f:
    lines = list(map(lambda l: l.strip(), f.readlines()))

from sklearn.metrics import precision_recall_curve as prc
from sklearn.metrics import auc

ys = []
probas = []
for l in lines:
    y, proba = l.split(',')
    y = 0 if y == '-' else 1
    proba = float(proba)
    ys.append(y)
    probas.append(proba)

P, R, T = prc(ys, probas)
print(auc(R, P))
